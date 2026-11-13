use super::*;
use crate::packet;
use aktor::worker::WorkerError;
use std::borrow::Cow;

pub async fn drive<I: Send + 'static, O: Send + 'static>(
    inner: Arc<Inner>,
    shared: Arc<Shared<I, O>>,
    operation: Operation,
    encode: fn(&I) -> Result<Vec<u8>, Failure>,
    decode: fn(&[u8]) -> Result<O, WorkerError>,
) {
    tokio::select! {
        _ = shared.cancel.notified() => {},
        _ = run(&inner, &shared, operation, encode, decode) => {},
    }
    shared.finish();
}

async fn run<I: Send, O: Send>(
    inner: &Inner,
    shared: &Shared<I, O>,
    operation: Operation,
    encode: fn(&I) -> Result<Vec<u8>, Failure>,
    decode: fn(&[u8]) -> Result<O, WorkerError>,
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
            Ok(None) => budget(shared, inner.open(), operation).await,
            Err(error) => Err(error),
        };
        let result = match opened {
            Ok(mut active) => {
                let Some(next) = generation.checked_add(1) else {
                    shared.fail(Failure::new(
                        CallError::NotAdmitted,
                        Cause::Closed,
                        "latest connection generations exhausted",
                    ));
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
                        .await?;
                        match inner
                            .frames
                            .read(&mut active.stream, inner.options.max_frame)
                            .await?
                        {
                            Message::LatestReady { session: ready } => {
                                session = Some(ready);
                                return Ok(ready);
                            }
                            Message::LatestExpired if session.is_some() => session = None,
                            Message::Reject(error) => return Err(error),
                            _ => {
                                return Err(Failure::new(
                                    CallError::NotAdmitted,
                                    Cause::Protocol,
                                    "expected latest session acknowledgement",
                                ));
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
        let Err(mut error) = result else { continue };
        let Some(current) = shared.pending() else {
            continue;
        };
        if current.attempts == 0 {
            continue;
        }
        error.outcome = current.outcome;
        let recoverable = match error.cause {
            Cause::Connection | Cause::Timeout | Cause::Closed => true,
            Cause::Codec | Cause::Protocol => false,
        };
        if !recoverable || current.attempts >= current.options.attempts {
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

async fn budget<I, O, T>(
    shared: &Shared<I, O>,
    future: impl Future<Output = Result<T, Failure>>,
    operation: Operation,
) -> Result<T, Failure> {
    tokio::pin!(future);
    loop {
        let Some(update) = shared.pending() else {
            return Err(Failure::new(
                CallError::NotAdmitted,
                Cause::Closed,
                "latest receiver closed",
            ));
        };
        shared.attempt(update.revision);

        tokio::select! {
            output = &mut future => return output,
            _ = shared.changed.notified() => {},
            _ = sleep_until(update.deadline) => return Err(expired(&update, operation)),
        }
    }
}

fn expired(update: &Update, operation: Operation) -> Failure {
    Failure::new(
        update.outcome,
        Cause::Timeout,
        format!("{} latest update timed out", operation.name),
    )
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
    encode: fn(&I) -> Result<Vec<u8>, Failure>,
    decode: fn(&[u8]) -> Result<O, WorkerError>,
    session: [u8; 16],
) -> Result<(), Failure> {
    let limit = inner.options.max_frame;
    let compress = active.compress;
    let (mut reader, mut writer) = tokio::io::split(active.stream);
    shared.seen.send_replace(0);
    let reading = async {
        loop {
            match inner.frames.read(&mut reader, limit).await? {
                Message::LatestSeen { revision } => {
                    acknowledge(shared, revision);
                }
                Message::LatestAnswer { revision, output } => {
                    acknowledge(shared, revision);
                    if !shared
                        .pending()
                        .is_some_and(|update| update.revision == revision)
                    {
                        continue;
                    }
                    let output = output.and_then(|bytes| decode(&bytes).map_err(Into::into));
                    shared.publish(revision, output);
                }
                Message::Reject(error) => return Err(error),
                _ => {
                    return Err(Failure::new(
                        CallError::OutcomeUnknown,
                        Cause::Protocol,
                        "expected latest reply",
                    ));
                }
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
                    _ = sleep_until(update.deadline) => return Err(Failure::new(update.outcome, Cause::Timeout, "remote latest update timed out")),
                }
            }

            let Some(update) = shared.encode(update.revision, encode) else {
                continue;
            };
            let Some(input) = update.input else {
                continue;
            };
            shared.attempt(update.revision);

            let bytes = input.and_then(|input| {
                wire::encode(
                    &Message::Update {
                        session,
                        revision: update.revision,
                        input: Cow::Borrowed(&input),
                    },
                    active.max_frame.min(limit),
                )
            });
            let bytes = match bytes {
                Ok(bytes) => bytes,
                Err(error) => {
                    shared.publish(update.revision, Err(error));
                    return Ok(());
                }
            };
            let bytes = if compress {
                packet::encode(bytes)
            } else {
                bytes
            };
            timeout_at(update.deadline, wire::write_bytes(&mut writer, &bytes))
                .await
                .map_err(|_| {
                    Failure::new(
                        update.outcome,
                        Cause::Timeout,
                        "remote latest write timed out",
                    )
                })??;

            let mut slot = shared.lock();
            if let Some(pending) = &mut slot.pending
                && pending.update.revision == update.revision
            {
                pending.update.outcome = CallError::OutcomeUnknown;
            }
            sent = Some(update.revision);
        }
    };

    tokio::select! {
        result = reading => result,
        result = writing => result,
    }
}
