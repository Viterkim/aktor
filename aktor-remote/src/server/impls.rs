use super::*;
use crate::packet;

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            build: String::new(),
            max_frame: wire::MAX_FRAME,
            max_connections: 32,
            shutdown_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(5),
            permissions: 0o600,
        }
    }
}

impl Server {
    pub async fn listen_with<S: 'static, Role: 'static>(
        address: impl Into<Address>,
        actor: &Aktor<S, Role>,
        options: ServerOptions,
    ) -> Result<Self, Error> {
        let service = Service {
            registry: Registry::for_remote::<S, Role>()?,
            run: |registry, state, name, input| Box::pin(registry.dispatch(state, name, input)),
            #[cfg(test)]
            pending: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cancelled: Arc::new(tokio::sync::Notify::new()),
        };
        Self::start(address.into(), actor, options, service).await
    }

    #[cfg(feature = "testing")]
    pub async fn listen_mock<S: 'static, Role: 'static>(
        address: impl Into<Address>,
        actor: &aktor::testing::Mock<S, Role>,
        options: ServerOptions,
    ) -> Result<Self, Error> {
        let service = Service {
            registry: Registry::for_remote::<S, Role>()?,
            run: |registry, state, name, input| {
                Box::pin(registry.dispatch_mock(state, name, input))
            },
            #[cfg(test)]
            pending: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cancelled: Arc::new(tokio::sync::Notify::new()),
        };
        Self::start(address.into(), &actor.actor, options, service).await
    }

    async fn start<S: 'static, Role: 'static>(
        address: Address,
        actor: &Aktor<S, Role>,
        options: ServerOptions,
        service: Service<S>,
    ) -> Result<Self, Error> {
        wire::validate(options.max_frame)?;
        if options.max_connections == 0
            || options.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || options.shutdown_timeout.is_zero()
            || options.io_timeout.is_zero()
        {
            return Err(Error::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "invalid connection limit or timeout",
            ));
        }
        let bind::Bound {
            listener,
            address,
            socket,
        } = bind::bind(address, &options).await?;
        let registry = Arc::new(service);
        #[cfg(test)]
        let pending = registry.pending.clone();
        #[cfg(test)]
        let cancelled = registry.cancelled.clone();
        let (stop, _) = watch::channel(false);
        let stopping = stop.clone();
        let (finished, completion) = watch::channel(None);
        let (errors, _) = broadcast::channel(64);
        let diagnostics = errors.clone();
        let handle = actor.new_handle();

        tokio::spawn(async move {
            let result = drive(listener, handle, registry, options, stopping, diagnostics).await;
            drop(socket);
            finished.send_replace(Some(result));
        });

        Ok(Self {
            address,
            stop,
            finished: completion,
            errors,
            #[cfg(test)]
            pending,
            #[cfg(test)]
            cancelled,
        })
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        self.stop.send_replace(true);
        self.wait().await
    }

    /// Observe protocol failures without stopping the service. Ordinary disconnects are omitted.
    pub fn errors(&self) -> broadcast::Receiver<Error> {
        self.errors.subscribe()
    }

    pub async fn wait(&self) -> Result<(), Error> {
        let mut finished = self.finished.clone();

        loop {
            if let Some(result) = finished.borrow_and_update().clone() {
                return result;
            }

            if finished.changed().await.is_err() {
                return Err(Error::new(
                    CallError::OutcomeUnknown,
                    Cause::Closed,
                    "remote listener stopped without a result",
                ));
            }
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
impl Drop for SocketPath {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path)
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

async fn drive<S: 'static, Role: 'static>(
    listener: Listener,
    handle: Handle<S, Role>,
    registry: Arc<Service<S>>,
    options: ServerOptions,
    stop: watch::Sender<bool>,
    errors: broadcast::Sender<Error>,
) -> Result<(), Error> {
    let mut stopping = stop.subscribe();
    let mut clients = JoinSet::new();
    let capacity = Arc::new(tokio::sync::Semaphore::new(options.max_connections));
    let sessions = Arc::new(super::sessions::Sessions::default());
    let mut result = loop {
        if *stopping.borrow() {
            break Ok(());
        }

        tokio::select! {
            _ = stopping.changed() => break Ok(()),
            _ = handle.wait_closing() => break Err(Error::new(CallError::OutcomeUnknown, Cause::Closed, "served actor is closing")),
            joined = clients.join_next(), if !clients.is_empty() => {
                if let Some(joined) = joined
                    && let Err(error) = session_result(joined, &errors)
                {
                    break Err(error);
                }
            }
            // One extra handshake can retire a latest connection at capacity.
            accepted = listener.accept(), if clients.len() <= options.max_connections => {
                let stream = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error.into()),
                };
                let handle = handle.new_handle();
                let registry = registry.clone();
                let options = options.clone();
                let stopping = stopping.clone();
                let sessions = sessions.clone();
                let capacity = capacity.clone();

                clients.spawn(async move {
                    session(stream, handle, registry, options, stopping, sessions, capacity).await
                });
            }
        }
    };
    stop.send_replace(true);

    let draining = async {
        while let Some(joined) = clients.join_next().await {
            if let Err(failed) = session_result(joined, &errors)
                && result.is_ok()
            {
                result = Err(failed);
            }
        }
    };

    match timeout(options.shutdown_timeout, draining).await {
        Ok(()) => {}
        Err(_) => {
            clients.abort_all();
            while clients.join_next().await.is_some() {}

            return Err(Error::new(
                CallError::OutcomeUnknown,
                Cause::Timeout,
                "remote sessions did not finish before shutdown deadline",
            ));
        }
    }

    result
}

fn session_result(
    joined: Result<Result<(), Error>, tokio::task::JoinError>,
    errors: &broadcast::Sender<Error>,
) -> Result<(), Error> {
    match joined {
        Ok(Err(error)) if error.cause != Cause::Connection => {
            let _ = errors.send(error);
        }
        Err(error) => {
            return Err(Error::new(
                CallError::OutcomeUnknown,
                Cause::Closed,
                error.to_string(),
            ));
        }
        Ok(_) => {}
    }

    Ok(())
}

async fn session<S: 'static, Role: 'static>(
    mut stream: Stream,
    handle: Handle<S, Role>,
    registry: Arc<Service<S>>,
    options: ServerOptions,
    mut stopping: watch::Receiver<bool>,
    sessions: Arc<super::sessions::Sessions>,
    capacity: Arc<tokio::sync::Semaphore>,
) -> Result<(), Error> {
    if *stopping.borrow() {
        return Ok(());
    }

    let hello = tokio::select! {
        _ = stopping.changed() => return Ok(()),
        hello = timeout(options.io_timeout, wire::read(&mut stream, wire::MAX_HANDSHAKE)) => {
            hello.map_err(|_| Error::new(CallError::NotAdmitted, Cause::Timeout, "remote handshake timed out"))??
        },
    };
    let mut output_limit = options.max_frame;
    let mismatch = match hello {
        Message::Hello {
            version,
            build,
            operations,
            max_frame,
        } => {
            wire::validate(max_frame as usize)?;
            output_limit = output_limit.min(max_frame as usize);

            if version != wire::VERSION {
                Some((
                    "remote protocol mismatch",
                    format!(
                        "remote protocol {version}, server expects {}",
                        wire::VERSION
                    ),
                ))
            } else if build != options.build {
                Some((
                    "remote build mismatch",
                    format!("remote build {build:?}, server expects {:?}", options.build),
                ))
            } else if !registry.operations().supports(&operations) {
                Some((
                    "remote operation catalogue mismatch",
                    format!(
                        "operation catalogue mismatch\nserver: {:?}\nclient: {:?}",
                        registry.operations().0,
                        operations.0
                    ),
                ))
            } else {
                None
            }
        }
        _ => Some((
            "expected remote handshake",
            "expected remote handshake".into(),
        )),
    };

    if let Some((summary, diagnostics)) = mismatch {
        let error = Error::new(CallError::NotAdmitted, Cause::Protocol, diagnostics);
        write_response(
            &mut stream,
            &Message::Reject(Error::new(CallError::NotAdmitted, Cause::Protocol, summary)),
            output_limit,
            options.io_timeout,
        )
        .await?;
        return Err(error);
    }

    write_response(
        &mut stream,
        &Message::Ready {
            max_frame: options.max_frame as u32,
        },
        output_limit,
        options.io_timeout,
    )
    .await?;

    let mut slot = capacity.clone().try_acquire_owned().ok();
    let compress = stream.compresses();
    let (mut reader, mut writer) = tokio::io::split(stream);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }

        let message = tokio::select! {
            _ = stopping.changed() => return Ok(()),
            message = async {
                let frame = wire::read_idle(&mut reader, options.max_frame, options.io_timeout);
                if slot.is_some() {
                    frame.await
                } else {
                    timeout(options.io_timeout, frame).await.map_err(|_| Error::new(
                        CallError::NotAdmitted, Cause::Timeout, "remote replacement handshake timed out",
                    ))?
                }
            } => message?,
        };
        if slot.is_none() {
            slot = capacity.clone().try_acquire_owned().ok();
        }
        if let Message::Latest {
            operation,
            session,
            generation,
        } = message
        {
            if slot.is_none() && session.is_none() {
                return Err(Error::new(
                    CallError::NotAdmitted,
                    Cause::Closed,
                    "remote connection limit reached",
                ));
            }
            let claim = match sessions.claim(session, generation).await {
                Ok(claim) => claim,
                Err(error) => {
                    wire::write_reply(
                        &mut writer,
                        &wire::encode(
                            &Message::Reject(Error::new(
                                error.outcome,
                                error.cause,
                                "latest session refused",
                            )),
                            output_limit,
                        )?,
                        options.io_timeout,
                    )
                    .await?;
                    return Err(error);
                }
            };
            let Some(mut claim) = claim else {
                wire::write_reply(
                    &mut writer,
                    &wire::encode(&Message::LatestExpired, output_limit)?,
                    options.io_timeout,
                )
                .await?;
                continue;
            };
            if slot.is_none() {
                slot = Some(
                    timeout(options.io_timeout, capacity.clone().acquire_owned())
                        .await
                        .map_err(|_| {
                            Error::new(
                                CallError::NotAdmitted,
                                Cause::Timeout,
                                "remote replacement capacity timed out",
                            )
                        })?
                        .map_err(|_| {
                            Error::new(
                                CallError::NotAdmitted,
                                Cause::Closed,
                                "remote listener closed",
                            )
                        })?,
                );
            }
            #[cfg(test)]
            let cancelled = registry.cancelled.clone();
            let result = tokio::select! {
                result = async move {
                    let _slot = slot;
                    super::latest::serve(reader.unsplit(writer), handle, registry, super::latest::Latest { operation, session: claim.session }, options, output_limit, stopping).await
                } => result,
                _ = claim.cancel.changed() => Err(Error::new(CallError::OutcomeUnknown, Cause::Closed, "latest connection superseded")),
            };
            #[cfg(test)]
            cancelled.notify_one();
            return result;
        }
        let Message::Call {
            id,
            operation,
            input,
        } = message
        else {
            return Err(Error::new(
                CallError::Discarded,
                Cause::Protocol,
                "expected remote call",
            ));
        };
        if slot.is_none() {
            let error = Error::new(
                CallError::NotAdmitted,
                Cause::Closed,
                "remote connection limit reached",
            );
            wire::write_reply(
                &mut writer,
                &wire::encode(
                    &Message::Answer {
                        id,
                        output: Err(error.clone()),
                    },
                    output_limit,
                )?,
                options.io_timeout,
            )
            .await?;
            return Err(error);
        }
        let output = match registry.resolve(&operation) {
            Some(registered) => {
                let name = registered.name();
                #[cfg(test)]
                registry.pending.notify_one();
                #[cfg(test)]
                let cancelled = registry.cancelled.clone();
                let registry = registry.clone();
                let request = call_async(
                    &handle,
                    async move |state: &mut S, ()| {
                        registry
                            .dispatch(state, operation, input)
                            .await
                            .map_err(Error::from)
                    },
                    (),
                )
                .operation(Operation {
                    name,
                    caller: std::panic::Location::caller(),
                });

                let admitted = tokio::select! {
                    biased;
                    error = disconnected(&mut reader) => {
                        #[cfg(test)]
                        cancelled.notify_one();
                        return Err(error);
                    },
                    _ = stopping.changed() => None,
                    _ = handle.wait_closing() => None,
                    reply = request.admit() => reply.ok(),
                };
                match admitted {
                    Some(reply) => {
                        wire::write_reply(
                            &mut writer,
                            &wire::encode(&Message::Admitted { id }, output_limit)?,
                            options.io_timeout,
                        )
                        .await?;
                        tokio::select! {
                            biased;
                            error = disconnected(&mut reader) => return Err(error),
                            output = reply.receive() => output.unwrap_or_else(|outcome| {
                                Err(Error::new(outcome, Cause::Closed, "served actor closed before replying"))
                            }),
                        }
                    }
                    None => Err(Error::new(
                        CallError::NotAdmitted,
                        Cause::Closed,
                        "remote service stopped before admission",
                    )),
                }
            }
            None => Err(Error::new(
                CallError::Discarded,
                Cause::Protocol,
                format!("unknown remote operation {operation:?}"),
            )),
        };

        let protocol = output
            .as_ref()
            .err()
            .filter(|error| error.cause == Cause::Protocol)
            .cloned();
        let answer = Message::Answer { id, output };
        let bytes = match wire::encode(&answer, output_limit) {
            Ok(bytes) => bytes,
            Err(mut error) => {
                error.outcome = CallError::OutcomeUnknown;
                wire::encode(
                    &Message::Answer {
                        id,
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
        if let Some(error) = protocol {
            return Err(error);
        }
    }
}

async fn write_response(
    stream: &mut Stream,
    message: &Message,
    limit: usize,
    duration: Duration,
) -> Result<(), Error> {
    timeout(duration, wire::write(stream, message, limit))
        .await
        .map_err(|_| {
            Error::new(
                CallError::NotAdmitted,
                Cause::Timeout,
                "remote handshake write timed out",
            )
        })?
}

async fn disconnected<R: tokio::io::AsyncRead + Unpin>(stream: &mut R) -> Error {
    use tokio::io::AsyncReadExt;

    match stream.read_u8().await {
        Err(error) => Error::from(error),
        Ok(_) => Error::new(
            CallError::OutcomeUnknown,
            Cause::Protocol,
            "remote client sent another frame before its reply",
        ),
    }
}
