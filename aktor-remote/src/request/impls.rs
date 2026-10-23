use super::*;
use aktor::call::{CastRequest, RetryRequest, SendRequest, TimeoutRequest};

impl<I, O, C> Request<I, O, C> {
    pub fn new(inner: Arc<Inner>, operation: Operation, input: I) -> Self {
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
                Error::new(
                    CallError::NotAdmitted,
                    Cause::Protocol,
                    "remote request already consumed",
                )
            })??;
            let bytes = inner
                .call(operation.name, input, options, Some(admitted))
                .await?;
            C::decode_output(&bytes).map_err(|error| {
                let mut error = Error::from(error);
                error.outcome = CallError::OutcomeUnknown;
                error
            })
        });
        self.running = Some(Execution {
            answer,
            admitted: received,
        });
    }

    pub async fn send(mut self) -> Result<Reply<O>, Error>
    where
        O: Send,
    {
        self.begin();
        let Some(Execution {
            mut answer,
            admitted: received,
        }) = self.running.take()
        else {
            return Err(Error::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "remote request has no answer",
            ));
        };
        tokio::select! {
            biased;
            result = received => {
                if result.is_err() {
                    let output = answer.await?;
                    return Ok(Reply::start(async move { Ok(output) }));
                }
                Ok(Reply::start(answer))
            }
            result = &mut answer => {
                let output = result?;
                Ok(Reply::start(async move { Ok(output) }))
            }
        }
    }
}

impl<I: Send, O: 'static, C: Codec<I> + Codec<O>> Future for Request<I, O, C> {
    type Output = Result<O, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.begin();

        match &mut this.running {
            Some(running) => running.answer.as_mut().poll(cx),
            None => Poll::Ready(Err(Error::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "remote request has no answer",
            ))),
        }
    }
}
impl<O: Send + 'static> Reply<O> {
    fn start(answer: impl Future<Output = Result<O, Error>> + Send + 'static) -> Self {
        Self {
            answer: tokio::spawn(answer),
        }
    }
}
impl<O> Future for Reply<O> {
    type Output = Result<O, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.answer).poll(cx).map(|result| {
            result.unwrap_or_else(|error| {
                Err(Error::new(
                    CallError::OutcomeUnknown,
                    Cause::Closed,
                    error.to_string(),
                ))
            })
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
    type Reply = Result<Reply<O>, Error>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<I: Send, C: Codec<I> + Codec<()>> CastRequest for Request<I, (), C> {
    type Output = Result<(), Error>;

    async fn cast(self) -> Self::Output {
        drop(self.send().await?);
        Ok(())
    }
}
