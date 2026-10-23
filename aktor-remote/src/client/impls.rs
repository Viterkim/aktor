use super::*;
use crate::packet;
use aktor::dispatch::{OwnedState, ReadState, WriteState};

impl<S: 'static, Role> ReadState<S, Role> for &Client<S, Role> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, Role> WriteState<S, Role> for &Client<S, Role> {
    type Lease = OwnedState<S>;
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            build: String::new(),
            timeout: Duration::from_secs(5),
            retry_delay: Duration::from_millis(100),
            max_frame: wire::MAX_FRAME,
        }
    }
}

impl<S: 'static, Role: 'static> Client<S, Role> {
    /// Create a handle without requiring the service to be available.
    pub fn new(address: impl Into<Address>) -> Self {
        Self::build(address.into(), ClientOptions::default())
    }

    pub fn with_options(
        address: impl Into<Address>,
        options: ClientOptions,
    ) -> Result<Self, Error> {
        wire::validate(options.max_frame)?;
        if options.timeout.is_zero() || options.retry_delay.is_zero() {
            return Err(Error::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "call timeout and retry delay must be positive",
            ));
        }
        Ok(Self::build(address.into(), options))
    }

    fn build(address: Address, options: ClientOptions) -> Self {
        Self {
            inner: Arc::new(Inner {
                address,
                operations: Operations::for_remote::<S, Role>(),
                connection: Mutex::new(Connection {
                    stream: None,
                    next: 1,
                }),
                options,
                #[cfg(feature = "udp")]
                udp: tokio::sync::OnceCell::new(),
            }),
            state: PhantomData,
        }
    }

    pub async fn connect_with(
        address: impl Into<Address>,
        options: ClientOptions,
    ) -> Result<Self, Error> {
        let client = Self::with_options(address, options)?;
        let stream = timeout(client.inner.options.timeout, client.inner.open())
            .await
            .map_err(|_| {
                Error::new(
                    CallError::NotAdmitted,
                    Cause::Timeout,
                    format!("connecting to {} timed out", client.inner.address),
                )
            })??;
        client.inner.connection.lock().await.stream = Some(stream);
        Ok(client)
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

impl<S, Role, I: Send, O: 'static, C> Transport<S, I, O, Role, C> for &Client<S, Role>
where
    C: Codec<I> + Codec<O>,
{
    type Request = Request<I, O, C>;

    fn request(self, operation: Operation, input: I) -> Self::Request {
        Request::new(self.inner.clone(), operation, input)
    }
}

impl Inner {
    pub fn take_idle(&self) -> Result<Option<Active>, Error> {
        let Ok(mut connection) = self.connection.try_lock() else {
            return Ok(None);
        };
        let Some(mut active) = connection.stream.take() else {
            return Ok(None);
        };
        if active.stream.idle()? {
            Ok(Some(active))
        } else {
            Ok(None)
        }
    }

    pub async fn open(&self) -> Result<Active, Error> {
        let mut stream = self.connect().await?;
        wire::write(
            &mut stream,
            &Message::Hello {
                version: wire::VERSION,
                build: self.options.build.clone(),
                operations: self.operations.clone(),
                max_frame: self.options.max_frame as u32,
            },
            wire::MAX_HANDSHAKE,
        )
        .await?;

        match wire::read(&mut stream, wire::MAX_HANDSHAKE).await? {
            Message::Ready { max_frame } => {
                wire::validate(max_frame as usize)?;
                Ok(Active {
                    stream,
                    max_frame: max_frame as usize,
                })
            }
            Message::Reject(error) => Err(error),
            _ => Err(Error::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "expected remote handshake",
            )),
        }
    }

    async fn connect(&self) -> Result<Stream, Error> {
        match &self.address {
            Address::Unix(path) => Ok(Stream::Unix(tokio::net::UnixStream::connect(path).await?)),
            Address::Tcp(address) => {
                let stream = tokio::net::TcpStream::connect(address).await?;
                stream.set_nodelay(true)?;
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
                    .await?;
                let stream = socket
                    .socket
                    .connect(*address)
                    .await
                    .map_err(std::io::Error::other)?;
                Ok(Stream::Udp(stream))
            }
        }
    }

    pub async fn call(
        &self,
        operation: &str,
        input: Vec<u8>,
        mut options: watch::Receiver<AktorRetryOpts>,
        mut admitted: Option<oneshot::Sender<()>>,
    ) -> Result<Vec<u8>, Error> {
        let mut outcome = CallError::NotAdmitted;
        let mut attempt = 0;
        let mut updating = true;
        loop {
            let started = Instant::now();
            let result = {
                let answer = self.attempt(operation, &input, &mut outcome, &mut admitted);
                tokio::pin!(answer);

                loop {
                    let current = *options.borrow_and_update();
                    if current.attempts == 0 || current.timeout.is_zero() {
                        break Ok(Err(Error::new(
                            CallError::NotAdmitted,
                            Cause::Protocol,
                            "retry attempts and timeout must be positive",
                        )));
                    }
                    let Some(deadline) = started.checked_add(current.timeout) else {
                        break Ok(Err(Error::new(
                            CallError::NotAdmitted,
                            Cause::Protocol,
                            "remote timeout is too large",
                        )));
                    };

                    tokio::select! {
                        biased;
                        changed = options.changed(), if updating => updating = changed.is_ok(),
                        result = &mut answer => break Ok(result),
                        _ = sleep_until(deadline) => break Err(()),
                    }
                }
            };
            let error = match result {
                Ok(Ok(output)) => return Ok(output),
                Ok(Err(mut error)) => {
                    error.outcome = outcome;
                    error
                }
                Err(_) => Error::new(
                    outcome,
                    Cause::Timeout,
                    format!("{operation} at {} timed out", self.address),
                ),
            };
            let recoverable = match error.cause {
                Cause::Connection | Cause::Timeout | Cause::Closed => true,
                Cause::Protocol | Cause::Codec => false,
            };
            attempt += 1;
            if attempt >= options.borrow().attempts || !recoverable {
                return Err(error);
            }
            sleep(self.options.retry_delay).await;

            let current = *options.borrow();
            if current.attempts == 0 || current.timeout.is_zero() {
                continue;
            }
            if attempt >= current.attempts {
                return Err(error);
            }
        }
    }

    async fn attempt(
        &self,
        operation: &str,
        input: &[u8],
        outcome: &mut CallError,
        admitted: &mut Option<oneshot::Sender<()>>,
    ) -> Result<Vec<u8>, Error> {
        let mut connection = self.connection.lock().await;
        let id = connection.next;
        connection.next = id.checked_add(1).ok_or_else(|| {
            Error::new(
                *outcome,
                Cause::Closed,
                "remote request identities exhausted",
            )
        })?;
        let bytes = wire::encode(
            &Message::Call {
                id,
                operation: operation.into(),
                input: input.to_vec(),
            },
            self.options.max_frame,
        )?;

        // A cancelled call owns and closes its stream, leaving no stale reply for the next call.
        let cached = match connection.stream.take() {
            Some(mut active) => {
                if active.stream.idle()? {
                    Some(active)
                } else {
                    None
                }
            }
            None => None,
        };
        let mut active = match cached {
            Some(stream) => stream,
            None => self.open().await?,
        };
        if bytes.len() > active.max_frame {
            return Err(Error::new(
                *outcome,
                Cause::Codec,
                "request exceeds the server's frame limit",
            ));
        }
        let bytes = if active.stream.compresses() {
            packet::encode(bytes)
        } else {
            bytes
        };
        let earlier = *outcome;
        wire::write_bytes(&mut active.stream, &bytes).await?;
        *outcome = CallError::OutcomeUnknown;
        let mut accepted = false;
        loop {
            match wire::read(&mut active.stream, self.options.max_frame).await? {
                Message::Admitted { id: answer } if answer == id && !accepted => {
                    accepted = true;
                    if let Some(admitted) = admitted.take() {
                        let _ = admitted.send(());
                    }
                }
                Message::Answer { id: answer, output } if answer == id => {
                    if let Err(error) = &output
                        && error.outcome != CallError::OutcomeUnknown
                        && earlier != CallError::OutcomeUnknown
                    {
                        *outcome = error.outcome;
                    }
                    if !accepted && output.is_ok() {
                        return Err(Error::new(
                            *outcome,
                            Cause::Protocol,
                            "remote answer arrived before admission",
                        ));
                    }
                    if output
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.cause == Cause::Closed)
                    {
                        return output;
                    }
                    connection.stream = Some(active);
                    return output;
                }
                _ => {
                    return Err(Error::new(
                        *outcome,
                        Cause::Protocol,
                        "expected matching remote reply",
                    ));
                }
            }
        }
    }
}
