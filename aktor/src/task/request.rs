use super::*;
use crate::{AktorTimeout, message::consumed, timeout::WaitStatus};
use core::task::{Context, Poll};
use std::time::Duration;

type Admission<'a> = AktorTaskFuture<'a, Result<SemaphorePermit<'a>, tokio::sync::AcquireError>>;
type Pending<S, I, Fut> = (I, fn(AktorTaskState<S>, I) -> Fut);

pub struct AktorTaskRequest<'a, S, I, O, Fut, Role = crate::AktorNoRole> {
    handle: &'a AktorTask<S, Role>,
    operation: Operation,
    pending: Option<Pending<S, I, Fut>>,
    admission: Option<Admission<'a>>,
    admitted: bool,
    reply: Option<ReplyState<O>>,
    parked: bool,
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
            reply: None,
            parked: false,
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
    fn submit<const REPLY: bool>(&mut self, slot: SemaphorePermit<'_>) -> Poll<()> {
        let Some((input, factory)) = self.pending.take() else {
            consumed();
        };
        let reply = REPLY.then(|| {
            let (sender, receiver) = oneshot::channel();
            self.reply = Some(ReplyState {
                receiver,
                parked: false,
                taken: false,
            });
            sender
        });

        let message = Message {
            operation: self.operation,
            job: Box::new(Call {
                input: Some(input),
                factory,
                reply,
                future: None,
            }),
        };

        // A receiver's waker can unwind after send has put the message in the queue.
        slot.forget();
        match self.handle.sender.send(message) {
            Ok(()) => {
                self.admitted = true;
                Poll::Ready(())
            }
            Err(error) => {
                self.handle.slots.add_permits(1);
                drop(error);
                self.park()
            }
        }
    }

    fn park(&mut self) -> Poll<()> {
        self.parked = true;
        self.handle.kill.stop();
        Poll::Pending
    }

    fn poll_submit<const REPLY: bool>(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.reply.as_ref().is_some_and(|reply| reply.taken) {
            consumed();
        }

        if self.admitted {
            return Poll::Ready(());
        }

        if self.parked {
            self.admission = None;
            return Poll::Pending;
        }

        if self.admission.is_none()
            && let Ok(slot) = self.handle.slots.try_acquire()
        {
            #[cfg(feature = "tokio")]
            core::task::ready!(core::pin::pin!(tokio::task::coop::consume_budget()).poll(cx));
            return self.submit::<REPLY>(slot);
        }

        let admission = self
            .admission
            .get_or_insert_with(|| Box::pin(self.handle.slots.acquire()));
        let result = match admission.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };

        self.admission = None;

        match result {
            Ok(slot) => self.submit::<REPLY>(slot),
            Err(_) => self.park(),
        }
    }

    pub async fn send(mut self) -> AktorTaskReply<O> {
        core::future::poll_fn(|cx| self.poll_submit::<true>(cx)).await;
        AktorTaskReply {
            state: self.reply.unwrap_or_else(|| consumed()),
            kill: self.handle.kill.clone(),
            clock: self.handle.clock,
        }
    }
}
impl<S, I, Fut, Role> AktorTaskRequest<'_, S, I, (), Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, ())> + Send + 'static,
{
    pub async fn cast(mut self) {
        core::future::poll_fn(|cx| self.poll_submit::<false>(cx)).await;
    }
}
impl<S, I, O, Fut, Role> Future for AktorTaskRequest<'_, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if this.poll_submit::<true>(cx).is_pending() {
            return Poll::Pending;
        }

        let reply = this.reply.as_mut().unwrap_or_else(|| consumed());
        reply.poll(cx, &this.handle.kill)
    }
}

pub struct AktorTaskReply<O> {
    state: ReplyState<O>,
    kill: AktorKillSwitch,
    clock: TaskClock,
}
impl<O> AktorTaskReply<O> {
    pub fn try_take(&mut self) -> Option<O> {
        if self.state.taken || self.state.parked {
            return None;
        }

        match self.state.receiver.try_recv() {
            Ok(output) => {
                self.state.taken = true;
                Some(output)
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                self.state.parked = true;
                self.kill.stop();
                None
            }
        }
    }

    pub fn timeout(&mut self, duration: Duration) -> AktorTimeout<&mut Self> {
        self.clock
            .timeout(self, duration, |_| WaitStatus { admitted: true })
    }
}
impl<O> Future for AktorTaskReply<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        this.state.poll(cx, &this.kill)
    }
}

struct ReplyState<O> {
    receiver: oneshot::Receiver<O>,
    parked: bool,
    taken: bool,
}
impl<O> ReplyState<O> {
    fn poll(&mut self, cx: &mut Context<'_>, kill: &AktorKillSwitch) -> Poll<O> {
        if self.taken {
            consumed();
        }

        if self.parked {
            return Poll::Pending;
        }

        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Err(_)) => {
                self.parked = true;
                kill.stop();
                Poll::Pending
            }
            Poll::Ready(Ok(output)) => {
                self.taken = true;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
