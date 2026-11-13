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
    ) -> Result<Self, (Error, String)> {
        let registry = Registry::for_remote::<S, Role>().map_err(Failure::from)?;
        let service = Service {
            operations: registry.operations(),
            registry,
            run: |registry, state, name, input| Box::pin(registry.dispatch(state, name, input)),
            #[cfg(test)]
            pending: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cancelled: Arc::new(tokio::sync::Notify::new()),
        };
        Ok(Self::start(address.into(), actor, options, service).await?)
    }

    #[cfg(feature = "testing")]
    pub async fn listen_mock<S: 'static, Role: 'static>(
        address: impl Into<Address>,
        actor: &aktor::testing::Mock<S, Role>,
        options: ServerOptions,
    ) -> Result<Self, (Error, String)> {
        let registry = Registry::for_remote::<S, Role>().map_err(Failure::from)?;
        let service = Service {
            operations: registry.operations(),
            registry,
            run: |registry, state, name, input| {
                Box::pin(registry.dispatch_mock(state, name, input))
            },
            #[cfg(test)]
            pending: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cancelled: Arc::new(tokio::sync::Notify::new()),
        };
        Ok(Self::start(address.into(), &actor.actor, options, service).await?)
    }

    async fn start<S: 'static, Role: 'static>(
        address: Address,
        actor: &Aktor<S, Role>,
        options: ServerOptions,
        service: Service<S>,
    ) -> Result<Self, Failure> {
        wire::validate(options.max_frame)?;
        if options.max_connections == 0
            || options.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || options.shutdown_timeout.is_zero()
            || options.io_timeout.is_zero()
        {
            return Err(Failure::new(
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
        let handle = actor.handle();

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

    pub async fn shutdown(&self) -> Result<(), (Error, String)> {
        self.stop.send_replace(true);
        self.wait().await
    }

    /// Observe protocol failures without stopping the service. The listener skips ordinary disconnects.
    pub fn errors(&self) -> broadcast::Receiver<(Error, String)> {
        self.errors.subscribe()
    }

    pub async fn wait(&self) -> Result<(), (Error, String)> {
        let mut finished = self.finished.clone();

        loop {
            if let Some(result) = finished.borrow_and_update().clone() {
                return Ok(result?);
            }

            if finished.changed().await.is_err() {
                return Err(Failure::new(
                    CallError::OutcomeUnknown,
                    Cause::Closed,
                    "remote listener stopped without a result",
                )
                .into());
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
    errors: broadcast::Sender<(Error, String)>,
) -> Result<(), Failure> {
    let mut stopping = stop.subscribe();
    let mut clients = JoinSet::new();
    let capacity = Arc::new(tokio::sync::Semaphore::new(options.max_connections));
    let sessions = Arc::new(super::sessions::Sessions::default());
    let mut accepting = core::pin::pin!(accept(&listener, &errors));
    let mut result = loop {
        if *stopping.borrow() {
            break Ok(());
        }

        tokio::select! {
            _ = stopping.changed() => break Ok(()),
            _ = handle.wait_closing() => break Err(Failure::new(CallError::OutcomeUnknown, Cause::Closed, "served actor is closing")),
            joined = clients.join_next(), if !clients.is_empty() => {
                if let Some(joined) = joined
                    && let Err(error) = session_result(joined, &errors)
                {
                    break Err(error);
                }
            }
            // One extra handshake can retire a latest connection at capacity.
            accepted = &mut accepting, if clients.len() <= options.max_connections => {
                let stream = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                accepting.set(accept(&listener, &errors));

                let handle = handle.clone();
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

            return Err(Failure::new(
                CallError::OutcomeUnknown,
                Cause::Timeout,
                "remote sessions did not finish before shutdown deadline",
            ));
        }
    }

    result
}

async fn accept(
    listener: &Listener,
    errors: &broadcast::Sender<(Error, String)>,
) -> Result<Stream, Failure> {
    use rustix::io::Errno;
    use std::io::ErrorKind;

    loop {
        let (error, backoff) = match listener.accept().await {
            Ok(stream) => match stream.configure() {
                Ok(()) => return Ok(stream),
                Err(error) => (error, false),
            },
            Err(error) => {
                let errno = error.raw_os_error().map(Errno::from_raw_os_error);
                let backoff = error.kind() == ErrorKind::OutOfMemory
                    || matches!(
                        errno,
                        Some(Errno::MFILE | Errno::NFILE | Errno::NOMEM | Errno::NOBUFS)
                    );
                #[cfg(target_os = "linux")]
                let pending_network_error = matches!(listener, Listener::Tcp(_))
                    && matches!(
                        errno,
                        Some(
                            Errno::PROTO
                                | Errno::NETDOWN
                                | Errno::NOPROTOOPT
                                | Errno::HOSTDOWN
                                | Errno::NONET
                                | Errno::HOSTUNREACH
                                | Errno::OPNOTSUPP
                                | Errno::NETUNREACH
                        )
                    );
                #[cfg(not(target_os = "linux"))]
                let pending_network_error = false;
                let transient = matches!(
                    error.kind(),
                    ErrorKind::Interrupted
                        | ErrorKind::WouldBlock
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset
                        | ErrorKind::TimedOut
                );

                if !transient && !backoff && !pending_network_error {
                    return Err(error.into());
                }

                (error, backoff)
            }
        };

        let _ = errors.send(Failure::from(error).into());
        if backoff {
            tokio::time::sleep(Duration::from_millis(50)).await;
        } else {
            tokio::task::coop::consume_budget().await;
        }
    }
}

fn session_result(
    joined: Result<Result<(), Failure>, tokio::task::JoinError>,
    errors: &broadcast::Sender<(Error, String)>,
) -> Result<(), Failure> {
    match joined {
        Ok(Err(error)) if error.cause != Cause::Connection => {
            let _ = errors.send(error.into());
        }
        Err(error) => {
            return Err(Failure::new(
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
    stream: Stream,
    handle: Handle<S, Role>,
    registry: Arc<Service<S>>,
    options: ServerOptions,
    mut stopping: watch::Receiver<bool>,
    sessions: Arc<super::sessions::Sessions>,
    capacity: Arc<tokio::sync::Semaphore>,
) -> Result<(), Failure> {
    if *stopping.borrow() {
        return Ok(());
    }

    let mut stream = stream.buffered();
    let frames = wire::FrameReader::default();
    let hello = tokio::select! {
        _ = stopping.changed() => return Ok(()),
        hello = timeout(options.io_timeout, frames.read_handshake(&mut stream)) => {
            hello.map_err(|_| Failure::new(CallError::NotAdmitted, Cause::Timeout, "remote handshake timed out"))??
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
                    format!(
                        "remote build {:?}, server expects {:?}",
                        preview(&build),
                        preview(&options.build),
                    ),
                ))
            } else {
                operations
                    .0
                    .iter()
                    .find(|operation| registry.operations.0.binary_search(operation).is_err())
                    .map(|operation| {
                        (
                            "remote operation catalogue mismatch",
                            format!(
                                "operation catalogue mismatch: unsupported operation {:?}",
                                preview(operation)
                            ),
                        )
                    })
            }
        }
        _ => Some((
            "expected remote handshake",
            "expected remote handshake".into(),
        )),
    };

    if let Some((summary, diagnostics)) = mismatch {
        let error = Failure::new(CallError::NotAdmitted, Cause::Protocol, diagnostics);
        write_response(
            &mut stream,
            &Message::Reject(Failure::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                summary,
            )),
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
    let compress = stream.get_ref().compresses();
    loop {
        if *stopping.borrow() {
            return Ok(());
        }

        let message = tokio::select! {
            _ = stopping.changed() => return Ok(()),
            message = async {
                let frame = frames.read_idle(&mut stream, options.max_frame, options.io_timeout);
                if slot.is_some() {
                    frame.await
                } else {
                    timeout(options.io_timeout, frame).await.map_err(|_| Failure::new(
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
                return Err(Failure::new(
                    CallError::NotAdmitted,
                    Cause::Closed,
                    "remote connection limit reached",
                ));
            }
            let claim = tokio::select! {
                _ = stopping.changed() => return Ok(()),
                claim = sessions.claim(session, generation, &mut slot) => claim,
            };
            let claim = match claim {
                Ok(claim) => claim,
                Err(error) => {
                    wire::write_reply(
                        &mut stream,
                        &wire::encode(
                            &Message::Reject(Failure::new(
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
                    &mut stream,
                    &wire::encode(&Message::LatestExpired, output_limit)?,
                    options.io_timeout,
                )
                .await?;
                if slot.is_none() {
                    return Ok(());
                }

                continue;
            };
            #[cfg(test)]
            let cancelled = registry.cancelled.clone();
            let result = tokio::select! {
                result = super::latest::serve(stream, handle, registry, super::latest::Latest { operation, session: claim.session, frames }, options, output_limit, stopping) => result,
                _ = claim.cancel.changed() => Err(Failure::new(CallError::OutcomeUnknown, Cause::Closed, "latest connection superseded")),
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
            return Err(Failure::new(
                CallError::Discarded,
                Cause::Protocol,
                "expected remote call",
            ));
        };
        if slot.is_none() {
            let error = Failure::new(
                CallError::NotAdmitted,
                Cause::Closed,
                "remote connection limit reached",
            );
            wire::write_reply(
                &mut stream,
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
                            .dispatch(state, operation.into_owned(), input.into_owned())
                            .await
                            .map_err(Failure::from)
                    },
                    (),
                )
                .operation(Operation {
                    name,
                    caller: std::panic::Location::caller(),
                });

                let admitted = tokio::select! {
                    biased;
                    error = disconnected(&mut stream) => {
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
                            &mut stream,
                            &wire::encode(&Message::Admitted { id }, output_limit)?,
                            options.io_timeout,
                        )
                        .await?;
                        tokio::select! {
                            biased;
                            error = disconnected(&mut stream) => return Err(error),
                            output = reply.receive_settled() => output.unwrap_or_else(|outcome| {
                                Err(Failure::new(outcome, Cause::Closed, "served actor closed before replying"))
                            }),
                        }
                    }
                    None => Err(Failure::new(
                        CallError::NotAdmitted,
                        Cause::Closed,
                        "remote service stopped before admission",
                    )),
                }
            }
            None => Err(Failure::new(
                CallError::Discarded,
                Cause::Protocol,
                format!("unknown remote operation {:?}", preview(&operation)),
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
            Err(error) => {
                let error = if let Some(protocol) = &protocol {
                    Failure::new(
                        protocol.outcome,
                        protocol.cause,
                        "remote operation rejected",
                    )
                } else {
                    Failure {
                        outcome: CallError::OutcomeUnknown,
                        ..error
                    }
                };
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
        wire::write_reply(&mut stream, &bytes, options.io_timeout).await?;
        if let Some(error) = protocol {
            return Err(error);
        }
    }
}

async fn write_response(
    stream: &mut BufferedStream,
    message: &Message<'_>,
    limit: usize,
    duration: Duration,
) -> Result<(), Failure> {
    timeout(duration, wire::write(stream, message, limit))
        .await
        .map_err(|_| {
            Failure::new(
                CallError::NotAdmitted,
                Cause::Timeout,
                "remote handshake write timed out",
            )
        })?
}

async fn disconnected<R: tokio::io::AsyncRead + Unpin>(stream: &mut R) -> Failure {
    use tokio::io::AsyncReadExt;

    match stream.read_u8().await {
        Err(error) => Failure::from(error),
        Ok(_) => Failure::new(
            CallError::OutcomeUnknown,
            Cause::Protocol,
            "remote client sent another frame before its reply",
        ),
    }
}
