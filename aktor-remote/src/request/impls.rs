use super::*;
use aktor::call::{CastRequest, RetryRequest, SendRequest, TimeoutRequest};

impl AktorRetryOpts {
    pub(crate) fn deadline(
        self,
        started: tokio::time::Instant,
    ) -> Result<tokio::time::Instant, Failure> {
        if self.attempts == 0 || self.timeout.is_zero() {
            return Err(Failure::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "retry attempts and timeout must be positive",
            ));
        }

        started.checked_add(self.timeout).ok_or_else(|| {
            Failure::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "remote timeout is too large",
            )
        })
    }
}

impl<I, O, C> Request<I, O, C> {
    pub(crate) fn new(inner: Arc<Inner>, operation: Operation, input: I) -> Self {
        let timeout = inner.options.timeout;
        Self {
            inner,
            operation,
            input: Some(Box::new(input)),
            options: watch::channel(AktorRetryOpts {
                attempts: 1,
                timeout,
            })
            .0,
            running: None,
            codec: PhantomData,
        }
    }

    pub fn retry(self, options: AktorRetryOpts) -> Self {
        self.options.send_replace(options);
        self
    }

    pub fn timeout(self, duration: Duration) -> Self {
        self.options
            .send_modify(|options| options.timeout = duration);
        self
    }
}

impl<I: Send, O: 'static, C: Codec<I> + Codec<O>> Request<I, O, C> {
    fn begin(&mut self) {
        if self.running.is_some() {
            return;
        }

        let (admitted, received) = oneshot::channel();
        let input = self
            .input
            .take()
            .map(|input| <C as Codec<I>>::encode(&input));
        let inner = self.inner.clone();
        let operation = self.operation;
        let options = self.options.subscribe();

        let answer: Answer<O> = Box::pin(async move {
            let input = input.ok_or_else(|| {
                Failure::new(
                    CallError::NotAdmitted,
                    Cause::Protocol,
                    "remote request already consumed",
                )
            })??;
            let bytes = inner
                .call(operation.name, input, options, Some(admitted))
                .await?;
            C::decode_output(&bytes).map_err(|error| {
                let mut error = Failure::from(error);
                error.outcome = CallError::OutcomeUnknown;
                error
            })
        });
        self.running = Some(Execution {
            answer,
            admitted: received,
        });
    }

    pub async fn send(mut self) -> Result<Reply<O>, (Error, String)>
    where
        O: Send,
    {
        self.begin();
        let Some(Execution {
            mut answer,
            admitted: mut received,
        }) = self.running.take()
        else {
            return Err(Failure::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "remote request has no answer",
            )
            .into());
        };
        tokio::select! {
            biased;
            result = &mut received => {
                if result.is_err() {
                    let output = answer.await?;
                    return Ok(Reply::start(async move { Ok(output) }));
                }
                Ok(Reply::start(answer))
            }
            result = &mut answer => {
                // One read can deliver both admission and a failed reply.
                if received.try_recv().is_ok() {
                    return Ok(Reply::start(async move { result }));
                }

                let output = result?;
                Ok(Reply::start(async move { Ok(output) }))
            }
        }
    }
}

impl<I: Send, O: 'static, C: Codec<I> + Codec<O>> Future for Request<I, O, C> {
    type Output = Result<O, (Error, String)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.begin();

        match &mut this.running {
            Some(running) => running.answer.as_mut().poll(cx).map_err(Into::into),
            None => Poll::Ready(Err(Failure::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "remote request has no answer",
            )
            .into())),
        }
    }
}
impl<O: Send + 'static> Reply<O> {
    fn start(answer: impl Future<Output = Result<O, Failure>> + Send + 'static) -> Self {
        Self {
            answer: tokio::spawn(answer),
        }
    }
}
impl<O> Future for Reply<O> {
    type Output = Result<O, (Error, String)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.answer).poll(cx).map(|result| {
            let output = result.unwrap_or_else(|error| {
                Err(Failure::new(
                    CallError::OutcomeUnknown,
                    Cause::Closed,
                    error.to_string(),
                ))
            });

            output.map_err(Into::into)
        })
    }
}
impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        self.answer.abort();
    }
}

impl<I, O, C> RetryRequest for Request<I, O, C> {
    type Options = AktorRetryOpts;

    fn retry(self, options: Self::Options) -> Self {
        self.retry(options)
    }
}
impl<I, O, C> TimeoutRequest for Request<I, O, C> {
    type Timed = Self;

    fn timeout(self, duration: Duration) -> Self {
        self.timeout(duration)
    }
}
impl<I: Send, O: Send + 'static, C: Codec<I> + Codec<O>> SendRequest for Request<I, O, C> {
    type Reply = Result<Reply<O>, (Error, String)>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<I: Send, C: Codec<I> + Codec<()>> CastRequest for Request<I, (), C> {
    type Output = Result<(), (Error, String)>;

    async fn cast(self) -> Self::Output {
        drop(self.send().await?);
        Ok(())
    }
}
