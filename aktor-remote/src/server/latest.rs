use super::*;
use crate::packet;
use aktor::latest::{SendLatest, Session};

pub struct Latest {
    pub operation: String,
    pub session: [u8; 16],
    pub frames: wire::FrameReader,
    pub errors: Failures,
}

#[derive(Er)]
pub struct ServeLatestErr {
    pub cause: Cause,
    pub msg: &'static str,
}
pub async fn serve<S: 'static, Role: 'static>(
    mut stream: BufferedStream,
    handle: Handle<S, Role>,
    registry: Arc<Service<S>>,
    latest: Latest,
    options: ServerOptions,
    output_limit: usize,
    mut stopping: watch::Receiver<bool>,
) -> ErResult<(), ServeLatestErr> {
    let Latest {
        operation,
        session,
        frames,
        errors,
    } = latest;

    let Some(registered) = registry.resolve(&operation) else {
        let rejected = WireFailure::rejected(
            AktorDeliveryOutcome::NotAdmitted,
            Cause::Protocol,
            Rejection::UnknownOperation,
        );
        errors.record(er_new!(
            ServerFailure::session(Cause::Protocol),
            UnknownOperationErr::new(preview(&operation))
        ));
        return reply(
            &mut stream,
            &Message::Reject(rejected),
            output_limit,
            &options,
        )
        .await;
    };

    reply(
        &mut stream,
        &Message::LatestReady { session },
        output_limit,
        &options,
    )
    .await?;

    let service = registry.clone();
    let (sender, mut results) = Session::session(
        &handle,
        Operation {
            name: registered.name(),
            caller: std::panic::Location::caller(),
        },
        async move |state: &mut S, (revision, input): (u64, Vec<u8>)| {
            let output = service.dispatch(state, operation.clone(), input).await;

            (revision, output)
        },
    );
    let compress = stream.get_ref().compresses();
    #[cfg(feature = "udp")]
    let pending = match stream.get_ref() {
        Stream::Udp(stream) => Some(stream.pending_receipt()),
        _ => None,
    };
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (seen, mut acknowledgements) = watch::channel(0);

    let mut reading = Box::pin(async move {
        let mut sender = Some(sender);
        let mut previous = 0;

        loop {
            if *stopping.borrow() {
                sender.take();
            }

            let frame = frames.read_idle(&mut reader, options.max_frame, options.io_timeout);
            tokio::pin!(frame);
            let message = tokio::select! {
                message = &mut frame => message,
                _ = stopping.changed(), if sender.is_some() => {
                    sender.take();
                    frame.await
                },
            };
            let Some(message) = message.er_with(|err| ServeLatestErr {
                cause: err.cause,
                msg: "read latest update",
            })?
            else {
                return ErResult::<_, ServeLatestErr>::Ok(None);
            };

            if let Message::DeliveryReceived(id) = message {
                return Ok(Some(id));
            }
            let Message::Update {
                session: identity,
                revision,
                input,
            } = message
            else {
                er_bail!(ServeLatestErr {
                    cause: Cause::Protocol,
                    msg: "expected latest update"
                });
            };

            if identity != session {
                er_bail!(ServeLatestErr {
                    cause: Cause::Protocol,
                    msg: "update belongs to a different latest session"
                });
            }

            if revision <= previous {
                er_bail!(
                    ServeLatestErr {
                        cause: Cause::Protocol,
                        msg: "latest revision did not advance"
                    },
                    RevisionErr {
                        previous,
                        received: revision
                    }
                );
            }

            let Some(sender) = &sender else { continue };
            sender.send((revision, input.into_owned()));
            previous = revision;
            seen.send_replace(revision);
        }
    });

    let (receipt, early) = {
        let offered = std::sync::OnceLock::new();
        let writing = async {
            let mut listening = true;

            loop {
                let answer = tokio::select! {
                    biased;
                    output = results.forward() => match output {
                        Some(Ok((revision, output))) => Ok(Message::LatestAnswer { revision, output: service::answer(output, &errors) }),
                        None => {
                            reply(&mut writer, &Message::LatestClosed, output_limit, &options).await?;
                            return ErResult::<_, ServeLatestErr>::Ok(None);
                        },
                        Some(Err(error)) => Err(error),
                    },
                    changed = acknowledgements.changed(), if listening => {
                        if changed.is_err() {
                            listening = false;
                            continue;
                        }

                        Ok(Message::LatestSeen { revision: *acknowledgements.borrow_and_update() })
                    }
                };

                let closed = answer.is_err();
                let answer = match answer {
                    Ok(answer) => answer,
                    Err(error) => {
                        Message::Reject(WireFailure::closed(error, Rejection::ActorClosed))
                    }
                };

                let (bytes, receipt) =
                    answer::encode(answer, output_limit, &errors).er(|| ServeLatestErr {
                        cause: Cause::Codec,
                        msg: "encode latest answer",
                    })?;
                if closed && let Some(id) = receipt {
                    let _ = offered.set(id);
                }
                let bytes = match compress {
                    true => packet::encode(bytes),
                    false => bytes,
                };

                wire::write_reply(&mut writer, &bytes, options.io_timeout)
                    .await
                    .er_with(|err| ServeLatestErr {
                        cause: err.cause,
                        msg: "send latest answer",
                    })?;
                if closed {
                    return Ok(receipt);
                }
            }
        };

        tokio::pin!(writing);
        tokio::select! {
            biased;
            result = &mut writing => (result?, None),
            result = &mut reading => {
                let Some(received) = result? else { return Ok(()) };
                if offered.get() != Some(&received) {
                    return Ok(());
                }
                (writing.await?, Some(received))
            }
        }
    };

    if let Some(id) = receipt {
        let receive = handle.delivery_receipt(id);
        tokio::spawn(async move {
            #[cfg(feature = "udp")]
            let _pending = pending;
            let received = match early {
                Some(id) => Some(id),
                None => reading.await.ok().flatten(),
            };
            if received == Some(id) {
                receive();
                let _ = timeout(
                    options.io_timeout,
                    wire::write(&mut writer, &Message::DeliveryReceived(id), output_limit),
                )
                .await;
            }
        });
    }
    Ok(())
}

#[derive(Er)]
struct RevisionErr {
    previous: u64,
    received: u64,
}

async fn reply<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut W,
    message: &Message<'_>,
    limit: usize,
    options: &ServerOptions,
) -> ErResult<(), ServeLatestErr> {
    let bytes = wire::encode(message, limit).er(|| ServeLatestErr {
        cause: Cause::Codec,
        msg: "encode latest reply",
    })?;

    wire::write_reply(stream, &bytes, options.io_timeout)
        .await
        .er_with(|err| ServeLatestErr {
            cause: err.cause,
            msg: "send latest reply",
        })
}
