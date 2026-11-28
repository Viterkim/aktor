use super::*;
use crate::{connection::IdleDataErr, packet};
use aktor::dispatch::{OwnedState, ReadState, RemoteCodec, WriteState};
use er::*;

impl<S: 'static, Role> ReadState<S, Role> for &Client<S, Role> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, Role> WriteState<S, Role> for &Client<S, Role> {
    type Lease = OwnedState<S>;
}

#[derive(Er)]
pub enum ClientOptionsError {
    FrameLimit,
    ZeroTimeout,
    ZeroRetryDelay,
}
fn check(options: &ClientOptions) -> ErResult<(), ClientOptionsError> {
    wire::validate(options.max_frame).er(ClientOptionsError::frame_limit)?;

    if options.timeout.is_zero() {
        er_bail!(ClientOptionsError::ZeroTimeout);
    }

    if options.retry_delay.is_zero() {
        er_bail!(ClientOptionsError::ZeroRetryDelay);
    }

    Ok(())
}

impl<S: 'static, Role: 'static> Client<S, Role> {
    /// Validate the options now and connect when the first call needs a session.
    pub fn new(
        address: Address,
        options: ClientOptions,
    ) -> Result<Self, (ClientOptionsError, String)> {
        check(&options)?;

        Ok(Self {
            inner: Arc::new(Inner {
                address,
                operations: Operations::for_remote::<S, Role>(),
                connection: Mutex::new(Connection {
                    idle: Vec::new(),
                    next: 1,
                }),
                options,
                frames: wire::FrameReader::default(),
                #[cfg(feature = "udp")]
                udp: tokio::sync::OnceCell::new(),
            }),
            state: PhantomData,
        })
    }
}
impl<S, Role> Clone for Client<S, Role> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            state: PhantomData,
        }
    }
}

impl<S, Role, I: Send, O: 'static, C> Transport<S, I, O, Role, RemoteCodec<C>> for &Client<S, Role>
where
    C: Codec<I> + Codec<O>,
{
    type Request = Request<I, O, C>;

    fn request(self, operation: Operation, input: I) -> Self::Request {
        Request::new(self.inner.clone(), operation, input)
    }
}

impl Inner {
    pub fn take_idle(&self) -> ErResult<Option<Active>, IdleDataErr> {
        let Ok(mut connection) = self.connection.try_lock() else {
            return Ok(None);
        };

        connection.take_idle()
    }

    async fn keep(&self, active: Active) {
        let mut connection = self.connection.lock().await;

        if connection.idle.len() < super::IDLE {
            connection.idle.push(active);
        }
    }
}

#[derive(Er)]
pub struct OpenErr {
    pub cause: Cause,
    pub msg: &'static str,
}
impl Inner {
    pub async fn open(&self) -> ErResult<Active, OpenErr> {
        let mut stream = self
            .connect()
            .await
            .er(|| OpenErr {
                cause: Cause::Connection,
                msg: "connect",
            })?
            .buffered();

        let hello = Message::Hello {
            version: wire::VERSION,
            build: self.options.build.clone(),
            operations: self.operations.clone(),
            max_frame: self.options.max_frame as u32,
        };

        wire::write(&mut stream, &hello, wire::MAX_HANDSHAKE)
            .await
            .er_with(|err| OpenErr {
                cause: err.cause,
                msg: "send handshake",
            })?;

        let answer = self
            .frames
            .read_handshake(&mut stream)
            .await
            .er_with(|err| OpenErr {
                cause: err.cause,
                msg: "read handshake",
            })?
            .er(|| OpenErr {
                cause: Cause::Connection,
                msg: "server closed during handshake",
            })?;

        match answer {
            Message::Ready { max_frame } => {
                wire::validate(max_frame as usize).er(|| OpenErr {
                    cause: Cause::Protocol,
                    msg: "server frame limit",
                })?;

                Ok(Active {
                    compress: stream.get_ref().compresses(),
                    stream,
                    max_frame: max_frame as usize,
                })
            }
            Message::Reject(rejected) => {
                er_bail!(
                    OpenErr {
                        cause: rejected.cause,
                        msg: "server rejected the handshake"
                    },
                    rejected.received()
                )
            }
            _ => er_bail!(OpenErr {
                cause: Cause::Protocol,
                msg: "expected remote handshake"
            }),
        }
    }
}

#[derive(Er)]
pub struct ConnectErr {
    pub address: String,
}
impl Inner {
    async fn connect(&self) -> ErResult<Stream, ConnectErr> {
        let e = |_| self.address.to_string();

        match &self.address {
            Address::Unix(path) => {
                let stream = tokio::net::UnixStream::connect(path).await.er(e)?;

                Ok(Stream::Unix(stream))
            }
            Address::Tcp(address) => {
                let stream = tokio::net::TcpStream::connect(address).await.er(e)?;

                stream.set_nodelay(true).er(e)?;
                Ok(Stream::Tcp(stream))
            }
            #[cfg(feature = "udp")]
            Address::Udp(address) => {
                let bind = match address {
                    std::net::SocketAddr::V4(_) => std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
                    std::net::SocketAddr::V6(_) => std::net::SocketAddr::from(([0_u16; 8], 0)),
                };
                let socket = self
                    .udp
                    .get_or_try_init(|| crate::udp::Socket::bind(bind, 32))
                    .await
                    .er(e)?;
                let stream = socket.connect(*address).await.er(e)?;

                Ok(Stream::Udp(stream))
            }
        }
    }
}

#[derive(Er)]
pub struct CallErr {
    pub failure_id: Option<aktor::FailureId>,
    pub operation: &'static str,
    pub outcome: AktorDeliveryOutcome,
    pub cause: Cause,
}
impl Inner {
    pub async fn call(
        &self,
        operation: &'static str,
        input: Vec<u8>,
        options: watch::Receiver<AktorRetryOptions>,
        admitted: Option<oneshot::Sender<()>>,
        receipt: &crate::receipt::Pending,
    ) -> ErResult<Vec<u8>, CallErr> {
        self.send(operation, input, options, admitted, false, receipt)
            .await
    }

    /// Done once the server admitted it. The connection goes straight back to the cache.
    pub async fn cast(
        &self,
        operation: &'static str,
        input: Vec<u8>,
        options: watch::Receiver<AktorRetryOptions>,
    ) -> ErResult<(), CallErr> {
        let receipt = crate::receipt::Pending::default();
        let result = self
            .send(operation, input, options, None, true, &receipt)
            .await;
        let confirmation = receipt.lock().unwrap().take();
        if let Some(confirmation) = confirmation {
            confirmation.send();
        }
        result?;

        Ok(())
    }

    async fn send(
        &self,
        operation: &'static str,
        input: Vec<u8>,
        mut options: watch::Receiver<AktorRetryOptions>,
        mut admitted: Option<oneshot::Sender<()>>,
        cast: bool,
        receipt: &crate::receipt::Pending,
    ) -> ErResult<Vec<u8>, CallErr> {
        let mut outcome = AktorDeliveryOutcome::NotAdmitted;
        let mut attempt = 0;
        let mut updating = true;

        loop {
            let started = Instant::now();
            let tried = {
                let answer = self.attempt(
                    operation,
                    &input,
                    &mut outcome,
                    &mut admitted,
                    cast,
                    receipt,
                );

                tokio::pin!(answer);

                loop {
                    let current = *options.borrow_and_update();
                    let deadline = match current.deadline(started) {
                        Ok(deadline) => deadline,
                        Err(error) => break Tried::Options(error),
                    };

                    tokio::select! {
                        biased;
                        changed = options.changed(), if updating => updating = changed.is_ok(),
                        result = &mut answer => break Tried::Answered(result),
                        _ = sleep_until(deadline) => break Tried::Elapsed,
                    }
                }
            };

            let error = match tried {
                Tried::Answered(Ok(output)) => return Ok(output),
                Tried::Answered(Err(error)) => {
                    let cause = error.top.cause();
                    let failure_id = match error.top {
                        AttemptErr::Server { failure_id, .. } => failure_id,
                        _ => None,
                    };

                    er_new!(
                        CallErr {
                            failure_id,
                            operation,
                            outcome,
                            cause
                        },
                        error
                    )
                }
                Tried::Options(error) => {
                    er_bail!(
                        CallErr {
                            failure_id: None,
                            operation,
                            outcome,
                            cause: Cause::Protocol
                        },
                        error
                    )
                }
                Tried::Elapsed => er_new!(CallErr {
                    failure_id: None,
                    operation,
                    outcome,
                    cause: Cause::Timeout
                }),
            };

            let recoverable = match error.top.cause {
                Cause::Connection | Cause::Timeout | Cause::Closed => true,
                Cause::Protocol | Cause::Codec => false,
            };

            attempt += 1;

            if attempt >= options.borrow().attempts.get()
                || !recoverable
                || error.top.failure_id.is_some()
            {
                return Err(error);
            }

            sleep(self.options.retry_delay).await;

            let current = *options.borrow();

            if current.timeout.is_zero() {
                continue;
            }

            if attempt >= current.attempts.get() {
                return Err(error);
            }
        }
    }
}

#[derive(Er)]
pub enum AttemptErr {
    IdsExhausted,
    IdleData,
    Encode,
    Open {
        cause: Cause,
    },
    TooLarge {
        size: usize,
        limit: usize,
    },
    Send,
    ReadReply {
        cause: Cause,
    },
    AnswerBeforeAdmission,
    Server {
        cause: Cause,
        failure_id: Option<aktor::FailureId>,
    },
    UnexpectedReply,
}
impl AttemptErr {
    fn cause(&self) -> Cause {
        match *self {
            Self::IdsExhausted => Cause::Closed,
            Self::IdleData | Self::AnswerBeforeAdmission | Self::UnexpectedReply => Cause::Protocol,
            Self::TooLarge { .. } | Self::Encode => Cause::Codec,
            Self::Send => Cause::Connection,
            Self::Open { cause } | Self::ReadReply { cause } | Self::Server { cause, .. } => cause,
        }
    }
}
impl Inner {
    async fn attempt(
        &self,
        operation: &str,
        input: &[u8],
        outcome: &mut AktorDeliveryOutcome,
        admitted: &mut Option<oneshot::Sender<()>>,
        cast: bool,
        receipt: &crate::receipt::Pending,
    ) -> ErResult<Vec<u8>, AttemptErr> {
        // Only the cached connection sits behind the lock. Holding it until the answer came
        // back would leave every other call on this client waiting on that one reply.
        let (id, cached) = {
            let mut connection = self.connection.lock().await;
            let id = connection.next;

            connection.next = id.checked_add(1).er(AttemptErr::ids_exhausted)?;

            // A cancelled call owns and closes its stream, leaving no stale reply for the next call.
            let cached = connection.take_idle().er(AttemptErr::idle_data)?;

            (id, cached)
        };

        let local_limit = self.options.max_frame;
        let limit = cached
            .as_ref()
            .map_or(local_limit, |active| active.max_frame.min(local_limit));
        let message = match cast {
            true => Message::Cast {
                id,
                operation: operation.into(),
                input: input.into(),
            },
            false => Message::Call {
                id,
                operation: operation.into(),
                input: input.into(),
            },
        };

        let bytes = match wire::encode(&message, limit) {
            Ok(bytes) => bytes,
            Err(error) => {
                if let Some(cached) = cached {
                    self.keep(cached).await;
                }

                er_bail!(AttemptErr::Encode, error);
            }
        };

        let active = match cached {
            Some(stream) => stream,
            None => self
                .open()
                .await
                .er_with(|err| AttemptErr::Open { cause: err.cause })?,
        };

        if bytes.len() > active.max_frame {
            let limit = active.max_frame;
            self.keep(active).await;

            er_bail!(AttemptErr::TooLarge {
                size: bytes.len(),
                limit
            });
        }

        let bytes = match active.compress {
            true => packet::encode(bytes),
            false => bytes,
        };
        let earlier = *outcome;

        let mut exchange = Exchange {
            client: self,
            active: None,
        };
        let active = exchange.active.insert(active);
        wire::write_bytes(&mut active.stream, &bytes)
            .await
            .er(AttemptErr::send)?;

        *outcome = AktorDeliveryOutcome::OutcomeUnknown;

        let mut accepted = false;

        loop {
            let reply = self
                .frames
                .read(&mut active.stream, self.options.max_frame)
                .await
                .er_with(|err| AttemptErr::ReadReply { cause: err.cause })?;

            match reply {
                Message::Admitted { id: answer } if answer == id && !accepted => {
                    accepted = true;

                    if let Some(admitted) = admitted.take() {
                        let _ = admitted.send(());
                    }

                    if cast {
                        exchange.keep().await;
                        return Ok(Vec::new());
                    }
                }
                Message::Answer { id: answer, output } if answer == id => {
                    if let Err(failure) = &output
                        && failure.outcome != AktorDeliveryOutcome::OutcomeUnknown
                        && earlier != AktorDeliveryOutcome::OutcomeUnknown
                    {
                        *outcome = failure.outcome;
                    }

                    if !accepted && output.is_ok() {
                        er_bail!(AttemptErr::AnswerBeforeAdmission);
                    }

                    let failure = match output {
                        Ok(output) => {
                            exchange.keep().await;
                            return Ok(output);
                        }
                        Err(failure) => failure,
                    };

                    if let Some(id) = failure
                        .failure_id
                        .filter(|_| failure.owner_report.is_some())
                    {
                        if let Some(active) = exchange.active.take() {
                            *receipt.lock().unwrap() = Some(crate::receipt::confirm(
                                active.stream,
                                id,
                                active.max_frame,
                                self.options.timeout,
                            ));
                        }
                    } else {
                        match failure.cause {
                            Cause::Closed | Cause::Protocol => {}
                            Cause::Connection | Cause::Timeout | Cause::Codec => {
                                exchange.keep().await
                            }
                        }
                    }

                    let cause = failure.cause;

                    er_bail!(
                        AttemptErr::Server {
                            cause,
                            failure_id: failure.failure_id
                        },
                        failure.received()
                    );
                }
                _ => {
                    er_bail!(AttemptErr::UnexpectedReply);
                }
            }
        }
    }
}

impl Exchange<'_> {
    async fn keep(&mut self) {
        if let Some(active) = self.active.take() {
            self.client.keep(active).await;
        }
    }
}
impl Drop for Exchange<'_> {
    fn drop(&mut self) {
        if let Some(active) = self.active.take() {
            crate::connection::finish(active.stream, self.client.options.timeout);
        }
    }
}

impl Connection {
    /// The newest idle connection that's still good, the older ones are likelier to be gone.
    fn take_idle(&mut self) -> ErResult<Option<Active>, IdleDataErr> {
        while let Some(mut active) = self.idle.pop() {
            if idle(&mut active.stream)? {
                return Ok(Some(active));
            }
        }

        Ok(None)
    }
}
