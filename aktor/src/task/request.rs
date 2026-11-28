use super::*;
use crate::{
    AktorTimeout,
    message::{AktorDeliveryOutcome, consumed},
    queue::mailbox,
    timeout::WaitStatus,
};
use core::task::{Context, Poll, ready};
use std::time::Duration;

type Admission<'a, S> = AktorTaskFuture<'a, Option<mailbox::Permit<'a, Message<S>>>>;
type Pending<S, I, Fut> = (I, fn(AktorTaskState<S>, I) -> Fut);

pub struct AktorTaskRequest<'a, S, I, O, Fut, Role = crate::AktorNoRole> {
    handle: &'a AktorTask<S, Role>,
    operation: Operation,
    pending: Option<Pending<S, I, Fut>>,
    admission: Option<Admission<'a, S>>,
    admitted: bool,
    closed: bool,
    reply: Option<ReplyState<O>>,
}
// Input moves into the queue. No field exposes it as pinned.
impl<S, I, O, Fut, Role> Unpin for AktorTaskRequest<'_, S, I, O, Fut, Role> {}
impl<'a, S, I, O, Fut, Role> AktorTaskRequest<'a, S, I, O, Fut, Role> {
    pub fn new(
        handle: &'a AktorTask<S, Role>,
        operation: Operation,
        input: I,
        factory: fn(AktorTaskState<S>, I) -> Fut,
    ) -> Self {
        Self {
            handle,
            operation,
            pending: Some((input, factory)),
            admission: None,
            admitted: false,
            closed: false,
            reply: None,
        }
    }

    pub fn timeout(self, duration: Duration) -> AktorTimeout<Self> {
        self.handle
            .clock
            .timeout(self, duration, |request| WaitStatus {
                admitted: request.admitted,
            })
    }
}
impl<'a, S, I, O, Fut, Role> AktorTaskRequest<'a, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    fn submit<const REPLY: bool>(&mut self, permit: mailbox::Permit<'a, Message<S>>) -> Poll<bool> {
        let Some((input, factory)) = self.pending.take() else {
            consumed();
        };
        let reply = REPLY.then(|| {
            let (sender, receiver) = oneshot::channel();
            self.reply = Some(ReplyState {
                receiver,

                taken: false,
            });
            sender
        });

        let message = Message {
            operation: self.operation,
            job: Box::pin(Call {
                input: Some(input),
                factory,
                admission: self.handle.inner.admission.clone(),
                reply,
                future: None,
            }),
        };

        match self.handle.inner.admission.admit(permit, message) {
            Ok(()) => {
                self.admitted = true;
                Poll::Ready(true)
            }
            Err(message) => {
                // Admission closed between the permit and the queue, the call never got in.
                self.reply = None;
                drop(message);
                self.refused()
            }
        }
    }

    fn refused(&mut self) -> Poll<bool> {
        self.pending = None;

        self.closed = true;
        Poll::Ready(false)
    }

    fn poll_submit<const REPLY: bool>(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if self.reply.as_ref().is_some_and(|reply| reply.taken) {
            consumed();
        }

        if self.admitted {
            return Poll::Ready(true);
        }

        if self.closed {
            return Poll::Ready(false);
        }

        let handle = self.handle;
        let inner = &handle.inner;

        if self.admission.is_none() {
            if inner.admission.phase().is_closing() {
                return self.refused();
            }

            if let Ok(permit) = inner.sender.try_reserve() {
                #[cfg(feature = "tokio")]
                ready!(core::pin::pin!(tokio::task::coop::consume_budget()).poll(cx));
                return self.submit::<REPLY>(permit);
            }

            self.admission = Some(Box::pin(async move { inner.sender.reserve().await.ok() }));
        }

        let Some(admission) = &mut self.admission else {
            consumed();
        };
        let result = ready!(admission.as_mut().poll(cx));

        self.admission = None;

        match result {
            Some(permit) => self.submit::<REPLY>(permit),
            None => self.refused(),
        }
    }

    pub fn send(self) -> AktorTaskSend<'a, S, I, O, Fut, Role> {
        AktorTaskSend(self)
    }
}
impl<'a, S, I, Fut, Role> AktorTaskRequest<'a, S, I, (), Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, ())> + Send + 'static,
{
    pub fn cast(self) -> AktorTaskCast<'a, S, I, Fut, Role> {
        AktorTaskCast(self)
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct AktorTaskSend<'a, S, I, O, Fut, Role = crate::AktorNoRole>(
    AktorTaskRequest<'a, S, I, O, Fut, Role>,
);
impl<S, I, O, Fut, Role> Unpin for AktorTaskSend<'_, S, I, O, Fut, Role> {}
impl<S, I, O, Fut, Role> Future for AktorTaskSend<'_, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Output = Result<AktorTaskReply<O>, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let request = &mut self.get_mut().0;
        if !ready!(request.poll_submit::<true>(cx)) {
            return Poll::Ready(Err(request
                .handle
                .inner
                .admission
                .call_error(AktorDeliveryOutcome::NotAdmitted, true)));
        }

        Poll::Ready(Ok(AktorTaskReply {
            state: request.reply.take().unwrap_or_else(|| consumed()),
            clock: request.handle.clock,
        }))
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct AktorTaskCast<'a, S, I, Fut, Role = crate::AktorNoRole>(
    AktorTaskRequest<'a, S, I, (), Fut, Role>,
);
impl<S, I, Fut, Role> Unpin for AktorTaskCast<'_, S, I, Fut, Role> {}
impl<S, I, Fut, Role> Future for AktorTaskCast<'_, S, I, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, ())> + Send + 'static,
{
    type Output = Result<(), crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let request = &mut self.get_mut().0;
        if ready!(request.poll_submit::<false>(cx)) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(request
                .handle
                .inner
                .admission
                .call_error(AktorDeliveryOutcome::NotAdmitted, true)))
        }
    }
}

impl<S, I, O, Fut, Role> Future for AktorTaskRequest<'_, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if !ready!(this.poll_submit::<true>(cx)) {
            return Poll::Ready(Err(this
                .handle
                .inner
                .admission
                .call_error(AktorDeliveryOutcome::NotAdmitted, true)));
        }

        let reply = this.reply.as_mut().unwrap_or_else(|| consumed());
        reply.poll(cx)
    }
}

pub struct AktorTaskReply<O> {
    state: ReplyState<O>,
    clock: Clock,
}
impl<O> AktorTaskReply<O> {
    pub fn try_take(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        if self.state.taken {
            return None;
        }

        let result = match self.state.receiver.try_recv() {
            Ok(result) => result,
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => {
                Err(crate::CallLoss(AktorDeliveryOutcome::OutcomeUnknown, None))
            }
        };
        self.state.taken = true;
        Some(match result {
            Ok(output) => Ok(output),
            Err(outcome) => Err(outcome.error(true)),
        })
    }

    pub fn timeout(&mut self, duration: Duration) -> AktorTimeout<&mut Self> {
        self.clock
            .timeout(self, duration, |_| WaitStatus { admitted: true })
    }
}
impl<O> Future for AktorTaskReply<O> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        this.state.poll(cx)
    }
}

struct ReplyState<O> {
    receiver: oneshot::Receiver<Result<O, crate::CallLoss>>,

    taken: bool,
}
impl<O> ReplyState<O> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<O, crate::AktorDeliveryError>> {
        if self.taken {
            consumed();
        }

        let output = ready!(Pin::new(&mut self.receiver).poll(cx)).unwrap_or(Err(crate::CallLoss(
            AktorDeliveryOutcome::OutcomeUnknown,
            None,
        )));
        self.taken = true;
        Poll::Ready(match output {
            Ok(value) => Ok(value),
            Err(outcome) => Err(outcome.error(true)),
        })
    }
}
