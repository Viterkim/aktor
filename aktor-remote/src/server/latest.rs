use super::*;
use crate::packet;
use aktor::latest::{SendLatest, Session};

pub struct Latest {
    pub operation: String,
    pub session: [u8; 16],
}

pub async fn serve<S: 'static, Role: 'static>(
    mut stream: Stream,
    handle: Handle<S, Role>,
    registry: Arc<Service<S>>,
    latest: Latest,
    options: ServerOptions,
    output_limit: usize,
    mut stopping: watch::Receiver<bool>,
) -> Result<(), Error> {
    let Latest { operation, session } = latest;
    let Some(registered) = registry.resolve(&operation) else {
        let error = Error::new(
            CallError::NotAdmitted,
            Cause::Protocol,
            format!("unknown latest operation {operation:?}"),
        );
        wire::write_reply(
            &mut stream,
            &wire::encode(
                &Message::Reject(Error::new(
                    error.outcome,
                    error.cause,
                    "unknown latest operation",
                )),
                output_limit,
            )?,
            options.io_timeout,
        )
        .await?;
        return Err(error);
    };
    wire::write_reply(
        &mut stream,
        &wire::encode(&Message::LatestReady { session }, output_limit)?,
        options.io_timeout,
    )
    .await?;
    let (sender, mut results) = Session::session(
        &handle,
        Operation {
            name: registered.name(),
            caller: std::panic::Location::caller(),
        },
        async move |state: &mut S, (revision, input): (u64, Vec<u8>)| {
            let output = registry
                .dispatch(state, operation.clone(), input)
                .await
                .map_err(Error::from);
            (revision, output)
        },
    );
    let compress = stream.compresses();
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (seen, mut acknowledgements) = watch::channel(0);
    let reading = async move {
        let mut previous = 0;
        loop {
            if *stopping.borrow() {
                return Ok::<_, Error>(());
            }
            let message = tokio::select! {
                _ = stopping.changed() => return Ok(()),
                message = wire::read_idle(&mut reader, options.max_frame, options.io_timeout) => message?,
            };
            let Message::Update {
                session: identity,
                revision,
                input,
            } = message
            else {
                return Err(Error::new(
                    CallError::Discarded,
                    Cause::Protocol,
                    "expected latest update",
                ));
            };
            if identity != session || revision <= previous {
                return Err(Error::new(
                    CallError::Discarded,
                    Cause::Protocol,
                    "latest session or revision does not match",
                ));
            }

            sender.send((revision, input));
            previous = revision;
            seen.send_replace(revision);
        }
    };
    let writing = async {
        let closed = handle.closed();
        tokio::pin!(closed);
        let mut listening = true;
        loop {
            let answer = tokio::select! {
                biased;
                output = results.next() => {
                    let Some((revision, output)) = output else { return Ok(()) };
                    Message::LatestAnswer { revision, output }
                }
                _ = &mut closed => {
                    let error = Error::new(CallError::OutcomeUnknown, Cause::Closed, "served actor closed before replying");
                    wire::write_reply(
                        &mut writer,
                        &wire::encode(&Message::Reject(error.clone()), output_limit)?,
                        options.io_timeout,
                    )
                    .await?;
                    return Err(error);
                }
                changed = acknowledgements.changed(), if listening => {
                    if changed.is_err() {
                        listening = false;
                        continue;
                    }
                    Message::LatestSeen { revision: *acknowledgements.borrow_and_update() }
                }
            };
            let bytes = match wire::encode(&answer, output_limit) {
                Ok(bytes) => bytes,
                Err(mut error) => {
                    error.outcome = CallError::OutcomeUnknown;
                    let Message::LatestAnswer { revision, .. } = answer else {
                        return Err(error);
                    };
                    wire::encode(
                        &Message::LatestAnswer {
                            revision,
                            output: Err(error),
                        },
                        output_limit,
                    )?
                }
            };
            let bytes = if compress {
                packet::encode(bytes)
            } else {
                bytes
            };
            wire::write_reply(&mut writer, &bytes, options.io_timeout).await?;
        }
    };

    tokio::try_join!(reading, writing)?;
    Ok(())
}
