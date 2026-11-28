use super::*;
use crate::{connection::AcceptStreamErr, packet};
use aktor::message::Reply;

trait Endpoint {
    type State: 'static;
    type Role: 'static;
    type Queue: 'static;
    type QueueRole: 'static;

    fn queue(&self) -> Handle<Self::Queue, Self::QueueRole>;
    const RUN: service::Run<Self::Queue>;
}

impl<S: 'static, Role: 'static> Endpoint for Aktor<S, Role> {
    type State = S;
    type Role = Role;
    type Queue = S;
    type QueueRole = Role;

    fn queue(&self) -> Handle<S, Role> {
        self.handle()
    }

    const RUN: service::Run<S> =
        |registry, state, name, input| Box::pin(registry.dispatch(state, name, input));
}

#[cfg(feature = "testing")]
impl<S: 'static, Role: 'static> Endpoint for aktor::testing::Mock<S, Role> {
    type State = S;
    type Role = Role;
    type Queue = aktor::testing::Handlers;
    type QueueRole = ();

    fn queue(&self) -> Handle<Self::Queue> {
        self.handler_queue()
    }

    const RUN: service::Run<Self::Queue> =
        |registry, handlers, name, input| Box::pin(registry.dispatch_mock(handlers, name, input));
}

impl Server {
    /// Serve the remote operations of an actor or a testing mock on its existing queue.
    #[allow(private_bounds)]
    pub async fn listen<A: Endpoint>(
        address: ListenAddress,
        actor: &A,
        options: ServerOptions,
    ) -> Result<Self, (ServerError, String)> {
        let registry = Registry::for_remote::<A::State, A::Role>()
            .er(|| ServerError::listen(Cause::Protocol))?;
        let service = Service {
            operations: registry.operations(),
            registry,
            run: A::RUN,
            #[cfg(test)]
            pending: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            cancelled: Arc::new(tokio::sync::Notify::new()),
        };

        Self::start(address, actor.queue(), options, service)
            .await
            .er_with(|err| ServerError::listen(err.cause()))
            .er_split()
    }
}

#[derive(Er)]
pub enum StartErr {
    FrameLimit,
    ConnectionLimit { connections: usize, maximum: usize },
    ZeroTimeout { shutdown: Duration, io: Duration },
    Bind,
}
impl StartErr {
    fn cause(&self) -> Cause {
        match self {
            Self::FrameLimit | Self::ConnectionLimit { .. } | Self::ZeroTimeout { .. } => {
                Cause::Protocol
            }
            Self::Bind => Cause::Connection,
        }
    }
}
impl Server {
    async fn start<S: 'static, Role: 'static>(
        address: ListenAddress,
        handle: aktor::listener::Handle<S, Role>,
        options: ServerOptions,
        service: Service<S>,
    ) -> ErResult<Self, StartErr> {
        wire::validate(options.max_frame).er(StartErr::frame_limit)?;

        if options.max_connections.get() > tokio::sync::Semaphore::MAX_PERMITS {
            er_bail!(StartErr::ConnectionLimit {
                connections: options.max_connections.get(),
                maximum: tokio::sync::Semaphore::MAX_PERMITS,
            });
        }

        if options.shutdown_timeout.is_zero() || options.io_timeout.is_zero() {
            er_bail!(StartErr::ZeroTimeout {
                shutdown: options.shutdown_timeout,
                io: options.io_timeout
            });
        }

        let bind::Bound {
            listener,
            address,
            socket,
        } = bind::bind(&address, &options).await.er(StartErr::bind)?;
        let registry = Arc::new(service);
        #[cfg(test)]
        let pending = registry.pending.clone();
        #[cfg(test)]
        let cancelled = registry.cancelled.clone();
        let (stop, _) = watch::channel(false);
        let stopping = stop.clone();
        let failures = Failures::default();
        let errors = failures.clone();
        let task = tokio::spawn(async move {
            drive(
                listener,
                handle,
                registry,
                options,
                stopping,
                errors.clone(),
            )
            .await;
            if let Some(mut socket) = socket
                && let Err(error) = socket.remove()
            {
                errors.record(er_new!(ServerFailure::listener(Cause::Connection), error));
            }
        });

        Ok(Self {
            address,
            stop,
            task: tokio::sync::Mutex::new(ListenerTask::Running(task)),
            failures,
            #[cfg(test)]
            pending,
            #[cfg(test)]
            cancelled,
        })
    }

    pub async fn shutdown(&self) -> Result<(), (ServerError, String)> {
        self.stop.send_replace(true);
        self.wait().await
    }

    pub async fn wait(&self) -> Result<(), (ServerError, String)> {
        let mut task = self.task.lock().await;
        match &mut *task {
            ListenerTask::Finished(result) => result.clone(),
            ListenerTask::Running(running) => {
                if let Err(error) = running.await {
                    self.failures
                        .record(er_new!(ServerFailure::listener(Cause::Closed), error));
                }
                let result = self.failures.finish();
                *task = ListenerTask::Finished(result.clone());
                result
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
#[derive(Er)]
struct RemoveSocketErr {
    path: PathBuf,
}
impl SocketPath {
    fn remove(&mut self) -> ErResult<(), RemoveSocketErr> {
        let Some(path) = self.path.take() else {
            return Ok(());
        };
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => er_bail!(path, error),
        };
        if metadata.dev() == self.device && metadata.ino() == self.inode {
            std::fs::remove_file(&path).er(|_| path)?;
        }
        Ok(())
    }
}
impl Drop for SocketPath {
    fn drop(&mut self) {
        let _ = self.remove();
    }
}

async fn drive<S: 'static, Role: 'static>(
    listener: Listener,
    handle: Handle<S, Role>,
    registry: Arc<Service<S>>,
    options: ServerOptions,
    stop: watch::Sender<bool>,
    errors: Failures,
) {
    let mut stopping = stop.subscribe();
    let mut clients = JoinSet::new();
    let (cut, deadline) = watch::channel(false);
    let casts = Casts {
        observers: Default::default(),
        deadline,
    };
    let capacity = Arc::new(tokio::sync::Semaphore::new(options.max_connections.get()));
    let sessions = Arc::new(super::sessions::Sessions::default());
    {
        let mut accepting = core::pin::pin!(accept(&listener, &errors));

        loop {
            if *stopping.borrow() {
                break;
            }

            tokio::select! {
                _ = stopping.changed() => break,
                _ = handle.wait_closing() => {
                    errors.record(er_new!(ServerFailure::listener(Cause::Closed), ActorClosing));
                    break;
                }
                joined = clients.join_next(), if !clients.is_empty() => {
                    if let Some(joined) = joined {
                        let lost = joined.is_err();
                        session_result(joined, &errors);
                        if lost {
                            break;
                        }
                    }
                }
                // One extra handshake can retire a latest connection at capacity.
                accepted = &mut accepting, if clients.len() <= options.max_connections.get() => {
                    let stream = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            errors.record(er_new!(ServerFailure::listener(Cause::Connection), error));
                            break;
                        }
                    };

                    accepting.set(accept(&listener, &errors));

                    let handle = handle.clone();
                    let registry = registry.clone();
                    let options = options.clone();
                    let stopping = stopping.clone();
                    let sessions = sessions.clone();
                    let capacity = capacity.clone();
                    let errors = errors.clone();
                    let casts = casts.clone();

                    clients.spawn(async move {
                        session(stream, handle, registry, options, stopping, sessions, capacity, (errors, casts))
                            .await
                    });
                }
            }
        }
    };

    #[cfg(feature = "udp")]
    let udp = match listener {
        Listener::Udp(socket) => Some(socket),
        Listener::Tcp(_) | Listener::Unix(_) => None,
    };
    #[cfg(not(feature = "udp"))]
    drop(listener);

    stop.send_replace(true);

    let stopped = tokio::time::Instant::now();

    let drain = async {
        while let Some(joined) = clients.join_next().await {
            session_result(joined, &errors);
        }
    };
    let sessions_timed_out = timeout(options.shutdown_timeout, drain).await.is_err();

    if sessions_timed_out {
        clients.abort_all();
        while let Some(joined) = clients.join_next().await {
            session_result(joined, &errors);
        }
    }

    // All sessions have returned, so every admitted cast has an observer now.
    let mut observers = std::mem::take(&mut *casts.observers.lock());
    let drain = async {
        while let Some(joined) = observers.join_next().await {
            cast_result(joined, &errors);
        }
    };
    let casts_timed_out = timeout(
        options.shutdown_timeout.saturating_sub(stopped.elapsed()),
        drain,
    )
    .await
    .is_err();
    if casts_timed_out {
        cut.send_replace(true);
        while let Some(joined) = observers.join_next().await {
            cast_result(joined, &errors);
        }
    }

    if sessions_timed_out || casts_timed_out {
        errors.record(er_new!(ServerFailure::ShutdownTimeout));
    }

    #[cfg(feature = "udp")]
    if let Some(socket) = udp {
        // The last replies may still need the socket for acknowledgements and retransmission.
        if !sessions_timed_out && !casts_timed_out {
            socket
                .settle(options.shutdown_timeout.saturating_sub(stopped.elapsed()))
                .await;
        }

        socket.close().await;
    }
}

#[derive(Er)]
struct ActorClosing;

#[derive(Er)]
pub struct AcceptErr;
async fn accept(listener: &Listener, errors: &Failures) -> ErResult<Stream, AcceptErr> {
    loop {
        let error = match listener.accept().await {
            Ok(stream) => return Ok(stream),
            Err(error) => error,
        };
        let backoff = match error.top {
            AcceptStreamErr::Retry => false,
            AcceptStreamErr::Backoff => true,
            AcceptStreamErr::Fatal => er_bail!(AcceptErr, error),
        };

        errors.record(er_new!(ServerFailure::listener(Cause::Connection), error));

        if backoff {
            tokio::time::sleep(Duration::from_millis(50)).await;
        } else {
            tokio::task::coop::consume_budget().await;
        }
    }
}

fn session_result(
    joined: Result<ErResult<(), ServeErr>, tokio::task::JoinError>,
    errors: &Failures,
) {
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => errors.record(er_new!(ServerFailure::session(error.top.cause), error)),
        Err(error) => {
            let cause = if error.is_cancelled() {
                Cause::Timeout
            } else {
                Cause::Closed
            };
            errors.record(er_new!(ServerFailure::session(cause), error));
        }
    }
}

#[derive(Clone)]
struct Casts {
    observers: Arc<parking_lot::Mutex<JoinSet<()>>>,
    deadline: watch::Receiver<bool>,
}

fn cast_result(joined: Result<(), tokio::task::JoinError>, errors: &Failures) {
    if let Err(error) = joined {
        errors.record(er_new!(ServerFailure::listener(Cause::Closed), error));
    }
}

#[derive(Er)]
pub enum HandshakeErr {
    ProtocolMismatch { remote: u32, expected: u32 },
    BuildMismatch { remote: String, expected: String },
    UnsupportedOperation { operation: String },
    NotHello,
    FrameLimit { limit: u32 },
}
impl HandshakeErr {
    fn reason(&self) -> Rejection {
        match *self {
            Self::ProtocolMismatch { remote, expected } => Rejection::ProtocolMismatch {
                received: remote,
                expected,
            },
            Self::BuildMismatch { .. } => Rejection::BuildMismatch,
            Self::UnsupportedOperation { .. } => Rejection::UnsupportedOperation,
            Self::NotHello => Rejection::ExpectedHello,
            Self::FrameLimit { limit } => Rejection::InvalidFrameLimit { limit },
        }
    }
}

#[derive(Er)]
pub struct ServeErr {
    pub cause: Cause,
    pub msg: &'static str,
}
#[allow(clippy::too_many_arguments)]
async fn session<S: 'static, Role: 'static>(
    stream: Stream,
    handle: Handle<S, Role>,
    registry: Arc<Service<S>>,
    options: ServerOptions,
    mut stopping: watch::Receiver<bool>,
    sessions: Arc<super::sessions::Sessions>,
    capacity: Arc<tokio::sync::Semaphore>,
    (errors, casts): (Failures, Casts),
) -> ErResult<(), ServeErr> {
    if *stopping.borrow() {
        return Ok(());
    }

    let mut stream = stream.buffered();
    let frames = wire::FrameReader::default();
    let hello = tokio::select! {
        _ = stopping.changed() => return Ok(()),
        hello = timeout(options.io_timeout, frames.read_handshake(&mut stream)) => hello,
    };
    let hello = hello.er(|| ServeErr {
        cause: Cause::Timeout,
        msg: "remote handshake timed out",
    })?;
    let Some(hello) = hello.er_with(|err| ServeErr {
        cause: err.cause,
        msg: "read handshake",
    })?
    else {
        return Ok(());
    };

    let mut output_limit = options.max_frame;
    let mismatch = match hello {
        Message::Hello {
            version,
            build,
            operations,
            max_frame,
        } => {
            output_limit = output_limit.min((max_frame as usize).max(wire::MIN_FRAME));
            if max_frame < wire::MIN_FRAME as u32 {
                Some(HandshakeErr::FrameLimit { limit: max_frame })
            } else if version != wire::VERSION {
                Some(HandshakeErr::ProtocolMismatch {
                    remote: version,
                    expected: wire::VERSION,
                })
            } else if build != options.build {
                Some(HandshakeErr::BuildMismatch {
                    remote: preview(&build).into_owned(),
                    expected: preview(&options.build).into_owned(),
                })
            } else {
                operations
                    .0
                    .iter()
                    .find(|operation| registry.operations.0.binary_search(operation).is_err())
                    .map(|operation| HandshakeErr::unsupported_operation(preview(operation)))
            }
        }
        _ => Some(HandshakeErr::NotHello),
    };

    if let Some(mismatch) = mismatch {
        let mut rejected = WireFailure::rejected(
            AktorDeliveryOutcome::NotAdmitted,
            Cause::Protocol,
            mismatch.reason(),
        );
        let mismatch = er_new!(mismatch);
        rejected.owner_report = Some(mismatch.er_report());
        errors.record(er_new!(ServerFailure::session(Cause::Protocol), mismatch));
        let (bytes, _) =
            answer::encode(Message::Reject(rejected), output_limit, &errors).er(|| ServeErr {
                cause: Cause::Codec,
                msg: "encode negotiation rejection",
            })?;
        return wire::write_reply(&mut stream, &bytes, options.io_timeout)
            .await
            .er_with(|err| ServeErr {
                cause: err.cause,
                msg: "send negotiation rejection",
            });
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

        let Some(message) = (tokio::select! {
            _ = stopping.changed() => return Ok(()),
            message = next_frame(&frames, &mut stream, &options, slot.is_some()) => message?,
        }) else {
            return Ok(());
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
            let claim = tokio::select! {
                _ = stopping.changed() => return Ok(()),
                claim = sessions.claim(session, generation, &mut slot) => claim,
            };
            let claim = match claim {
                Ok(claim) => claim,
                Err(error) => {
                    let rejected = WireFailure::rejected(
                        AktorDeliveryOutcome::NotAdmitted,
                        error.top.cause(),
                        error.top.reason(),
                    );
                    errors.record(er_new!(ServerFailure::session(error.top.cause()), error));
                    return reply(
                        &mut stream,
                        &Message::Reject(rejected),
                        output_limit,
                        &options,
                    )
                    .await;
                }
            };
            let Some(mut claim) = claim else {
                reply(&mut stream, &Message::LatestExpired, output_limit, &options).await?;

                if slot.is_none() {
                    return Ok(());
                }

                continue;
            };

            #[cfg(test)]
            let cancelled = registry.cancelled.clone();

            let latest = super::latest::Latest {
                operation,
                session: claim.session,
                frames,
                errors,
            };
            let result = tokio::select! {
                result = super::latest::serve(stream, handle, registry, latest, options, output_limit, stopping) => result.er_with(|err| ServeErr {
                    cause: err.cause,
                    msg: "serve latest session",
                }),
                _ = claim.cancel.changed() => Ok(()),
            };

            #[cfg(test)]
            cancelled.notify_one();

            return result;
        }

        let (id, operation, input, cast) = match message {
            Message::Call {
                id,
                operation,
                input,
            } => (id, operation, input, false),
            Message::Cast {
                id,
                operation,
                input,
            } => (id, operation, input, true),
            _ => {
                er_bail!(ServeErr {
                    cause: Cause::Protocol,
                    msg: "expected remote call"
                });
            }
        };

        let registered = match (slot.is_some(), registry.resolve(&operation)) {
            (true, Some(registered)) => registered,
            (available, _) => {
                let failure = if available {
                    WireFailure::rejected(
                        AktorDeliveryOutcome::NotAdmitted,
                        Cause::Protocol,
                        Rejection::UnknownOperation,
                    )
                } else {
                    WireFailure::rejected(
                        AktorDeliveryOutcome::NotAdmitted,
                        Cause::Closed,
                        Rejection::ConnectionLimit,
                    )
                };
                let kind = if cast {
                    ServerFailure::cast(preview(&operation), failure.outcome, failure.cause, None)
                } else {
                    ServerFailure::session(failure.cause)
                };
                errors.record(er_new!(
                    kind,
                    RejectedOperation::new(preview(&operation), failure.reason)
                ));

                let answer = Message::Answer {
                    id,
                    output: Err(failure),
                };
                reply(&mut stream, &answer, output_limit, &options).await?;
                return Ok(());
            }
        };
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
            },
            (),
        )
        .operation(Operation {
            name,
            caller: std::panic::Location::caller(),
        });

        let admitted = tokio::select! {
            biased;
            result = disconnected(&mut stream) => {
                #[cfg(test)]
                cancelled.notify_one();

                if cast {
                    let failure = match result {
                        Ok(()) => er_new!(ServerFailure::cast(name, AktorDeliveryOutcome::NotAdmitted, Cause::Connection, None)),
                        Err(error) => error.er_with(|err| ServerFailure::cast(name, AktorDeliveryOutcome::NotAdmitted, err.cause, None)),
                    };
                    errors.record(failure);
                    return Ok(());
                }
                return result;
            },
            reply = request.forward().send() => reply,
            _ = stopping.changed() => Err(AktorDeliveryOutcome::NotAdmitted.into()),
        };

        let output = match admitted {
            Ok(admitted) => {
                let acknowledgement = Message::Admitted { id };

                if cast {
                    observe_cast(&casts, &errors, name, admitted);
                    reply(&mut stream, &acknowledgement, output_limit, &options).await?;
                    continue;
                }

                reply(&mut stream, &acknowledgement, output_limit, &options).await?;
                tokio::select! {
                    biased;
                    result = disconnected(&mut stream) => return result,
                    output = admitted => match output {
                        Ok(output) => service::answer(output, &errors),
                        Err(error) => Err(WireFailure::closed(error, Rejection::ActorClosed)),
                    },
                }
            }
            Err(error) => {
                let failure = WireFailure::closed(error, Rejection::AdmissionRefused);
                if cast {
                    errors.record(er_new!(
                        ServerFailure::cast(
                            name,
                            failure.outcome,
                            failure.cause,
                            failure.failure_id
                        ),
                        failure.clone().received()
                    ));
                }
                Err(failure)
            }
        };

        let close = output
            .as_ref()
            .is_err_and(|failure| failure.cause == Cause::Protocol);
        let answer = Message::Answer { id, output };
        let (bytes, failure_id) =
            answer::encode(answer, output_limit, &errors).er(|| ServeErr {
                cause: Cause::Codec,
                msg: "encode answer",
            })?;
        let bytes = match compress {
            true => packet::encode(bytes),
            false => bytes,
        };

        wire::write_reply(&mut stream, &bytes, options.io_timeout)
            .await
            .er_with(|err| ServeErr {
                cause: err.cause,
                msg: "send answer",
            })?;

        if let Some(id) = failure_id {
            crate::receipt::listen(
                stream,
                id,
                options.max_frame,
                options.io_timeout,
                handle.delivery_receipt(id),
            );
            return Ok(());
        }
        if close {
            return Ok(());
        }
    }
}

/// Without a slot the client may only replace an old latest connection, and gets one timeout
/// for that.
async fn next_frame(
    frames: &wire::FrameReader,
    stream: &mut BufferedStream,
    options: &ServerOptions,
    slot: bool,
) -> ErResult<Option<Message<'static>>, ServeErr> {
    let frame = frames.read_idle(stream, options.max_frame, options.io_timeout);

    let frame = match slot {
        true => frame.await,
        false => timeout(options.io_timeout, frame).await.er(|| ServeErr {
            cause: Cause::Timeout,
            msg: "remote replacement handshake timed out",
        })?,
    };

    frame.er_with(|err| ServeErr {
        cause: err.cause,
        msg: "read frame",
    })
}

#[derive(Er)]
struct RejectedOperation {
    operation: String,
    reason: Rejection,
}

fn observe_cast(
    casts: &Casts,
    errors: &Failures,
    name: &'static str,
    admitted: Reply<ErResult<Vec<u8>, service::DispatchErr>>,
) {
    let mut observers = casts.observers.lock();
    while let Some(joined) = observers.try_join_next() {
        cast_result(joined, errors);
    }

    let errors = errors.clone();
    let mut deadline = casts.deadline.clone();
    observers.spawn(async move {
        let failure = tokio::select! {
            biased;
            result = admitted => match result {
                Ok(Ok(_)) => return,
                Ok(Err(error)) => er_new!(ServerFailure::cast(name, error.top.outcome, error.top.cause, None), error),
                Err(error) => er_new!(ServerFailure::cast(name, error.outcome, Cause::Closed, error.failure_id), error),
            },
            _ = deadline.changed() => er_new!(ServerFailure::cast(name, AktorDeliveryOutcome::OutcomeUnknown, Cause::Timeout, None)),
        };

        errors.record(failure);
    });
}

async fn reply(
    stream: &mut BufferedStream,
    message: &Message<'_>,
    limit: usize,
    options: &ServerOptions,
) -> ErResult<(), ServeErr> {
    let bytes = wire::encode(message, limit).er(|| ServeErr {
        cause: Cause::Codec,
        msg: "encode reply",
    })?;

    wire::write_reply(stream, &bytes, options.io_timeout)
        .await
        .er_with(|err| ServeErr {
            cause: err.cause,
            msg: "send reply",
        })
}

async fn write_response(
    stream: &mut BufferedStream,
    message: &Message<'_>,
    limit: usize,
    duration: Duration,
) -> ErResult<(), ServeErr> {
    let written = timeout(duration, wire::write(stream, message, limit))
        .await
        .er(|| ServeErr {
            cause: Cause::Timeout,
            msg: "remote handshake write timed out",
        })?;

    written.er_with(|err| ServeErr {
        cause: err.cause,
        msg: "send handshake reply",
    })
}

async fn disconnected<R: tokio::io::AsyncRead + Unpin>(stream: &mut R) -> ErResult<(), ServeErr> {
    use tokio::io::AsyncReadExt;

    let mut byte = [0];
    let read = stream.read(&mut byte).await.er(|| ServeErr {
        cause: Cause::Connection,
        msg: "watch the client during its call",
    })?;
    if read != 0 {
        er_bail!(ServeErr {
            cause: Cause::Protocol,
            msg: "remote client sent another frame before its reply",
        });
    }

    Ok(())
}
