use super::*;
use crate::packet;
use std::borrow::Cow;

pub async fn drive<I: Send + 'static, O: Send + 'static>(
    inner: Arc<Inner>,
    shared: Arc<Shared<I, O>>,
    operation: Operation,
    encode: fn(&I) -> CodecResult<Vec<u8>>,
    decode: fn(&[u8]) -> CodecResult<O>,
) {
    run(&inner, &shared, operation, encode, decode).await;

    shared.finish();
}

#[derive(Er)]
pub struct LatestErr {
    pub failure_id: Option<aktor::FailureId>,
    pub outcome: AktorDeliveryOutcome,
    pub cause: Cause,
    pub msg: &'static str,
}

/// The connection doesn't know how far the update got, `run` adds the outcome.
#[derive(Er)]
pub struct SessionErr {
    pub rejected: Option<(AktorDeliveryOutcome, Option<aktor::FailureId>, bool)>,
    pub cause: Cause,
    pub msg: &'static str,
}

async fn run<I: Send, O: Send>(
    inner: &Inner,
    shared: &Shared<I, O>,
    operation: Operation,
    encode: fn(&I) -> CodecResult<Vec<u8>>,
    decode: fn(&[u8]) -> CodecResult<O>,
) {
    let mut generation = 0_u64;
    let mut session = None;

    loop {
        if shared.closing() {
            break;
        }

        if shared.pending().is_none() {
            shared.changed.notified().await;
            continue;
        }

        let opened = match inner.take_idle() {
            Ok(Some(active)) => Ok(active),
            Ok(None) => {
                let opening = async {
                    inner.open().await.er_with(|err| SessionErr {
                        rejected: None,
                        cause: err.cause,
                        msg: "open connection",
                    })
                };

                budget(shared, opening, operation).await
            }
            Err(error) => Err(er_new!(
                SessionErr {
                    rejected: None,
                    cause: Cause::Protocol,
                    msg: "unexpected data on an idle connection"
                },
                error
            )),
        };

        let result = match opened {
            Ok(mut active) => {
                let Some(next) = generation.checked_add(1) else {
                    shared.fail(er_new!(LatestErr {
                        failure_id: None,
                        outcome: AktorDeliveryOutcome::NotAdmitted,
                        cause: Cause::Closed,
                        msg: "latest connection generations exhausted"
                    }));
                    return;
                };

                generation = next;

                let opening = async {
                    loop {
                        let start = Message::Latest {
                            operation: operation.name.into(),
                            session,
                            generation,
                        };

                        wire::write(
                            &mut active.stream,
                            &start,
                            active.max_frame.min(inner.options.max_frame),
                        )
                        .await
                        .er_with(|err| SessionErr {
                            rejected: None,
                            cause: err.cause,
                            msg: "start latest session",
                        })?;

                        let answer = inner
                            .frames
                            .read(&mut active.stream, inner.options.max_frame)
                            .await
                            .er_with(|err| SessionErr {
                                rejected: None,
                                cause: err.cause,
                                msg: "read latest acknowledgement",
                            })?;

                        match answer {
                            Message::LatestReady { session: ready } => {
                                session = Some(ready);
                                return Ok(ready);
                            }
                            Message::LatestExpired if session.is_some() => session = None,
                            Message::Reject(rejected) => {
                                let cause = rejected.cause;

                                er_bail!(
                                    SessionErr {
                                        rejected: Some((
                                            rejected.outcome,
                                            rejected.failure_id,
                                            rejected.owner_report.is_some()
                                        )),
                                        cause,
                                        msg: "the server refused the session"
                                    },
                                    rejected.received()
                                );
                            }
                            _ => {
                                er_bail!(SessionErr {
                                    rejected: None,
                                    cause: Cause::Protocol,
                                    msg: "expected latest session acknowledgement"
                                });
                            }
                        }
                    }
                };

                match budget(shared, opening, operation).await {
                    Ok(session) => connected(active, shared, inner, encode, decode, session).await,
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        };

        if shared.closing() {
            break;
        }

        let Err(error) = result else {
            continue;
        };
        if let Some((outcome, failure_id, _)) = error.top.rejected {
            let cause = error.top.cause;
            shared.fail(er_new!(
                LatestErr {
                    outcome,
                    cause,
                    failure_id,
                    msg: "server closed the latest session"
                },
                error
            ));
            return;
        }

        let Some(current) = shared.pending() else {
            let cause = error.top.cause;
            shared.fail(er_new!(
                LatestErr {
                    failure_id: None,
                    outcome: AktorDeliveryOutcome::OutcomeUnknown,
                    cause,
                    msg: "lost the latest connection",
                },
                error
            ));
            return;
        };

        if current.attempts == 0 {
            continue;
        }

        let recoverable = match error.top.cause {
            Cause::Connection | Cause::Timeout | Cause::Closed => true,
            Cause::Codec | Cause::Protocol => false,
        };

        if !recoverable || current.attempts >= current.options.attempts.get() {
            let cause = error.top.cause;
            let error = er_new!(
                LatestErr {
                    failure_id: None,
                    outcome: current.outcome,
                    cause,
                    msg: "latest session"
                },
                error
            );

            shared.publish(current.revision, Err(error));
            continue;
        }

        if backoff(shared, current.revision, inner.options.retry_delay).await {
            shared.retry(current.revision);
        }
    }
}

async fn backoff<I, O>(shared: &Shared<I, O>, revision: u64, delay: std::time::Duration) -> bool {
    let waiting = tokio::time::sleep(delay);

    tokio::pin!(waiting);

    loop {
        if shared.closing()
            || !shared
                .pending()
                .is_some_and(|update| update.revision == revision)
        {
            return false;
        }

        tokio::select! {
            _ = &mut waiting => return true,
            _ = shared.changed.notified() => {},
        }
    }
}

#[derive(Er)]
pub struct ExpiredErr {
    pub operation: &'static str,
}

/// Runs a step of the session until the pending update's deadline, or until there's nothing left
/// to send.
async fn budget<I, O, T>(
    shared: &Shared<I, O>,
    future: impl Future<Output = ErResult<T, SessionErr>>,
    operation: Operation,
) -> ErResult<T, SessionErr> {
    tokio::pin!(future);

    loop {
        let Some(update) = shared.pending() else {
            er_bail!(SessionErr {
                rejected: None,
                cause: Cause::Closed,
                msg: "latest receiver closed"
            });
        };

        shared.attempt(update.revision);

        tokio::select! {
            output = &mut future => return output,
            _ = shared.changed.notified() => {},
            _ = sleep_until(update.deadline) => er_bail!(
                SessionErr { rejected: None, cause: Cause::Timeout, msg: "latest update timed out" },
                ExpiredErr::new(operation.name)
            ),
        }
    }
}

fn acknowledge<I, O>(shared: &Shared<I, O>, revision: u64) {
    if shared.seen.send_if_modified(|seen| {
        if revision <= *seen {
            return false;
        }

        *seen = revision;
        true
    }) {
        shared.changed.notify_one();
    }
}

async fn connected<I: Send, O: Send>(
    active: Active,
    shared: &Shared<I, O>,
    inner: &Inner,
    encode: fn(&I) -> CodecResult<Vec<u8>>,
    decode: fn(&[u8]) -> CodecResult<O>,
    session: [u8; 16],
) -> ErResult<(), SessionErr> {
    let limit = inner.options.max_frame;
    let compress = active.compress;
    let (mut reader, mut writer) = tokio::io::split(active.stream);

    shared.seen.send_replace(0);

    let (result, abandoned): (ErResult<(), SessionErr>, bool) = {
        let reading = async {
            loop {
                let reply =
                    inner
                        .frames
                        .read(&mut reader, limit)
                        .await
                        .er_with(|err| SessionErr {
                            rejected: None,
                            cause: err.cause,
                            msg: "read latest reply",
                        })?;

                match reply {
                    Message::LatestClosed => {
                        if let Some(update) = shared.pending() {
                            shared.publish(
                                update.revision,
                                Err(er_new!(LatestErr {
                                    failure_id: None,
                                    outcome: update.outcome,
                                    cause: Cause::Closed,
                                    msg: "actor closed before answering the update",
                                })),
                            );
                        } else {
                            shared.finish();
                        }
                        return Ok(());
                    }
                    Message::LatestSeen { revision } => acknowledge(shared, revision),
                    Message::LatestAnswer { revision, output } => {
                        acknowledge(shared, revision);

                        if !shared
                            .pending()
                            .is_some_and(|update| update.revision == revision)
                        {
                            continue;
                        }

                        let output = match output {
                            Ok(bytes) => decode(&bytes).er(|| LatestErr {
                                failure_id: None,
                                outcome: AktorDeliveryOutcome::OutcomeUnknown,
                                cause: Cause::Codec,
                                msg: "decode latest reply",
                            }),
                            Err(failure) => {
                                let (outcome, cause) = (failure.outcome, failure.cause);

                                Err(er_new!(
                                    LatestErr {
                                        failure_id: failure.failure_id,
                                        outcome,
                                        cause,
                                        msg: "the server answered with a failure"
                                    },
                                    failure.received()
                                ))
                            }
                        };

                        shared.publish(revision, output);
                    }
                    Message::Reject(rejected) => {
                        let cause = rejected.cause;

                        er_bail!(
                            SessionErr {
                                rejected: Some((
                                    rejected.outcome,
                                    rejected.failure_id,
                                    rejected.owner_report.is_some()
                                )),
                                cause,
                                msg: "the server dropped the session"
                            },
                            rejected.received()
                        );
                    }
                    _ => er_bail!(SessionErr {
                        rejected: None,
                        cause: Cause::Protocol,
                        msg: "expected latest reply"
                    }),
                }
            }
        };

        let writing = async {
            let mut sent = None;

            loop {
                if shared.closing() {
                    return Ok(());
                }

                let Some(update) = shared.pending() else {
                    shared.changed.notified().await;
                    continue;
                };

                if let Some(sent) = sent
                    && (sent == update.revision || *shared.seen.borrow() < sent)
                {
                    tokio::select! {
                        _ = shared.changed.notified() => continue,
                        _ = sleep_until(update.deadline) => {
                            // A replacement can expire while the previous update still needs an acknowledgement.
                            shared.attempt(update.revision);
                            er_bail!(SessionErr { rejected: None, cause: Cause::Timeout, msg: "remote latest update timed out" });
                        },
                    }
                }

                let Some(update) = shared.encode(update.revision, encode) else {
                    continue;
                };
                let Some(input) = update.input else {
                    continue;
                };

                shared.attempt(update.revision);

                let input = match input {
                    Ok(input) => input,
                    Err(error) => {
                        let error = er_new!(
                            LatestErr {
                                failure_id: None,
                                outcome: update.outcome,
                                cause: Cause::Codec,
                                msg: "encode latest input"
                            },
                            error
                        );

                        shared.publish(update.revision, Err(error));
                        return Ok(());
                    }
                };

                let message = Message::Update {
                    session,
                    revision: update.revision,
                    input: Cow::Borrowed(&input),
                };
                let bytes = match wire::encode(&message, active.max_frame.min(limit)) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let error = er_new!(
                            LatestErr {
                                failure_id: None,
                                outcome: update.outcome,
                                cause: Cause::Codec,
                                msg: "encode latest update"
                            },
                            error
                        );

                        shared.publish(update.revision, Err(error));
                        return Ok(());
                    }
                };
                let bytes = match compress {
                    true => packet::encode(bytes),
                    false => bytes,
                };

                let written = timeout_at(update.deadline, wire::write_bytes(&mut writer, &bytes))
                    .await
                    .er(|| SessionErr {
                        rejected: None,
                        cause: Cause::Timeout,
                        msg: "remote latest write timed out",
                    })?;

                written.er(|| SessionErr {
                    rejected: None,
                    cause: Cause::Connection,
                    msg: "send latest update",
                })?;

                let mut slot = shared.lock();

                if let Some(pending) = &mut slot.pending
                    && pending.update.revision == update.revision
                {
                    pending.update.outcome = AktorDeliveryOutcome::OutcomeUnknown;
                }

                sent = Some(update.revision);
            }
        };

        tokio::select! {
            result = reading => (result, false),
            result = writing => (result, true),
            _ = shared.cancel.notified() => (Ok(()), true),
        }
    };
    if abandoned {
        crate::connection::finish(reader.unsplit(writer), inner.options.timeout);
    } else if let Err(error) = &result
        && let Some((_, Some(id), true)) = error.top.rejected
        && let Some(receipt) = shared.receipt.upgrade()
    {
        let stream = reader.unsplit(writer);
        *receipt.lock().unwrap() = Some(crate::receipt::confirm(
            stream,
            id,
            limit,
            inner.options.timeout,
        ));
    }
    result
}
