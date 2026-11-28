use super::*;
use aktor::call::{CastRequest, RetryRequest, SendRequest, TimeoutRequest};
use tokio::task::JoinError;

#[derive(Er)]
pub enum RetryOptionsErr {
    ZeroTimeout,
    DeadlineOverflow { timeout: Duration },
}
impl AktorRetryOptions {
    pub(crate) fn deadline(
        self,
        started: tokio::time::Instant,
    ) -> ErResult<tokio::time::Instant, RetryOptionsErr> {
        if self.timeout.is_zero() {
            er_bail!(RetryOptionsErr::ZeroTimeout);
        }

        started
            .checked_add(self.timeout)
            .er(|| RetryOptionsErr::deadline_overflow(self.timeout))
    }
}

impl<I, O, C> Request<I, O, C> {
    pub(crate) fn new(inner: Arc<Inner>, operation: Operation, input: I) -> Self {
        let timeout = inner.options.timeout;

        Self {
            inner,
            operation,
            input: Some(Box::new(input)),
            options: watch::channel(AktorRetryOptions {
                attempts: core::num::NonZeroUsize::MIN,
                timeout,
            })
            .0,
            running: None,
            codec: PhantomData,
        }
    }

    pub fn retry(self, options: AktorRetryOptions) -> Self {
        self.options.send_replace(options);
        self
    }

    pub fn timeout(self, duration: Duration) -> Self {
        self.options
            .send_modify(|options| options.timeout = duration);
        self
    }
}

#[derive(Er)]
pub enum RequestErr {
    AlreadyConsumed,
    EncodeInput,
    DecodeReply,
    ReplyLost,
    Call {
        outcome: AktorDeliveryOutcome,
        cause: Cause,
        failure_id: Option<aktor::FailureId>,
    },
    Cast {
        outcome: AktorDeliveryOutcome,
        cause: Cause,
        failure_id: Option<aktor::FailureId>,
    },
}
impl RequestErr {
    fn delivery(&self) -> (AktorDeliveryOutcome, Option<aktor::FailureId>) {
        match *self {
            Self::AlreadyConsumed | Self::EncodeInput => (AktorDeliveryOutcome::NotAdmitted, None),
            Self::DecodeReply | Self::ReplyLost => (AktorDeliveryOutcome::OutcomeUnknown, None),
            Self::Call {
                outcome,
                failure_id,
                ..
            }
            | Self::Cast {
                outcome,
                failure_id,
                ..
            } => (outcome, failure_id),
        }
    }
}

impl<I: Send, O: 'static, C: Codec<I> + Codec<O>> Request<I, O, C> {
    fn encode_input(&mut self) -> ErResult<Vec<u8>, RequestErr> {
        let input = self.input.take().er(RequestErr::already_consumed)?;
        <C as Codec<I>>::encode(&input).er(RequestErr::encode_input)
    }

    fn start(&mut self) -> Execution<O> {
        if let Some(running) = self.running.take() {
            return running;
        }

        let (admitted, received) = oneshot::channel();
        let input = self.encode_input();
        let inner = self.inner.clone();
        let operation = self.operation;
        let options = self.options.subscribe();

        let receipt = crate::receipt::Receipt::default();
        let pending = receipt.pending.clone();
        let answer: Answer<O> = Box::pin(async move {
            let input = input?;

            let bytes = inner
                .call(operation.name, input, options, Some(admitted), &pending)
                .await
                .er_with(|err| RequestErr::Call {
                    failure_id: err.failure_id,
                    outcome: err.outcome,
                    cause: err.cause,
                })?;

            C::decode(&bytes).er(RequestErr::decode_reply)
        });

        Execution {
            answer,
            admitted: received,
            receipt,
        }
    }

    pub async fn send(mut self) -> Result<Reply<O>, aktor::AktorDeliveryError>
    where
        O: Send,
    {
        finish_send(self.start()).await
    }
}

fn public<O>(result: ErResult<O, RequestErr>) -> Result<O, aktor::AktorDeliveryError> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let (outcome, failure_id) = error.top.delivery();
            Err(aktor::AktorDeliveryError {
                outcome,
                failure_id,
                report: error.er_report(),
            })
        }
    }
}

async fn finish_send<O: Send + 'static>(
    Execution {
        mut answer,
        admitted: mut received,
        mut receipt,
    }: Execution<O>,
) -> Result<Reply<O>, aktor::AktorDeliveryError> {
    let result = tokio::select! {
        biased;
        admission = &mut received => {
            if admission.is_ok() {
                return Ok(Reply::start(answer, receipt));
            }
            answer.await
        }
        result = &mut answer => {
            // One read can deliver both admission and a failed reply.
            if received.try_recv().is_ok() {
                return Ok(Reply::start(async move { result }, receipt));
            }
            result
        }
    };

    let mut result = Some(public(result));
    let output =
        std::future::poll_fn(|cx| receipt.poll(cx, |_| Poll::Ready(result.take().unwrap())))
            .await?;
    Ok(Reply::start(async move { Ok(output) }, receipt))
}

impl<I: Send, O: 'static, C: Codec<I> + Codec<O>> Future for Request<I, O, C> {
    type Output = Result<O, aktor::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let running = match &mut this.running {
            Some(running) => running,
            None => {
                let call = this.start();
                this.running.insert(call)
            }
        };

        running
            .receipt
            .poll(cx, |cx| running.answer.as_mut().poll(cx).map(public))
    }
}
impl<O: Send + 'static> Reply<O> {
    fn start(
        answer: impl Future<Output = ErResult<O, RequestErr>> + Send + 'static,
        receipt: crate::receipt::Receipt,
    ) -> Self {
        Self {
            receipt,
            answer: Some(tokio::spawn(answer)),
        }
    }
}
impl<O> Future for Reply<O> {
    type Output = Result<O, aktor::AktorDeliveryError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        this.receipt.poll(cx, |cx| {
            Pin::new(this.answer.as_mut().expect("reply already received"))
                .poll(cx)
                .map(finish)
        })
    }
}

/// A reply task that died says nothing about whether the call ran.
fn finish<O>(
    joined: Result<ErResult<O, RequestErr>, JoinError>,
) -> Result<O, aktor::AktorDeliveryError> {
    public(joined.er(RequestErr::reply_lost).and_then(|output| output))
}
impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        if let Some(answer) = &self.answer {
            answer.abort();
        }
    }
}

impl<I, O, C> RetryRequest for Request<I, O, C> {
    type Options = AktorRetryOptions;

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
impl<I: Send + 'static, O: Send + 'static, C: Codec<I> + Codec<O> + 'static> SendRequest
    for Request<I, O, C>
{
    type Reply = Reply<O>;
    type Send =
        Pin<Box<dyn Future<Output = Result<Self::Reply, aktor::AktorDeliveryError>> + Send>>;

    fn send(mut self) -> Self::Send {
        Box::pin(async move { finish_send(self.start()).await })
    }
}
impl<I: Send + 'static, C: Codec<I> + Codec<()> + 'static> CastRequest for Request<I, (), C> {
    type Output = Result<(), aktor::AktorDeliveryError>;
    type Cast = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn cast(mut self) -> Self::Cast {
        // A partially polled call may already be on the wire. Wait for its admission.
        if self.running.is_some() {
            let sent = <Self as SendRequest>::send(self);
            return Box::pin(async move {
                drop(sent.await?);
                Ok(())
            });
        }

        Box::pin(async move {
            let input = self.encode_input();
            let inner = self.inner;
            let operation = self.operation;
            let options = self.options.subscribe();

            let cast = async {
                let input = input?;

                inner
                    .cast(operation.name, input, options)
                    .await
                    .er_with(|err| RequestErr::Cast {
                        failure_id: err.failure_id,
                        outcome: err.outcome,
                        cause: err.cause,
                    })?;

                ErResult::Ok(())
            };

            public(cast.await)
        })
    }
}
