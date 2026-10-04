use super::*;
use crate::message::{TrySendError, consumed, stopped};
use core::{
    future::poll_fn,
    ops::AsyncFnOnce,
    pin::Pin,
    task::{Context, ready},
};

impl<'a, S: 'static, const N: usize, E, O: 'static, Role> Request<'a, S, N, E, O, Role> {
    pub fn new<F, I>(
        handle: &'a Handle<S, N, E, Role>,
        operation: Operation,
        function: F,
        input: I,
    ) -> Self
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + 'static,
        I: 'static,
    {
        let answer = Rc::new(Answer {
            result: RefCell::new(AnswerState::Waiting(None)),
        });

        Self {
            handle,
            message: Some(Message {
                _operation: operation,
                job: Box::new(Call {
                    function: Some((function, input)),
                    answer: answer.clone(),
                }),
            }),
            reply: Reply {
                parked: false,
                taken: false,
                answer,
                group: handle
                    .inner
                    .group
                    .borrow()
                    .as_ref()
                    .map(|(_, group)| group.clone()),
            },
            closed: handle.inner.closed.listen(),
            submitted: false,
        }
    }

    pub fn timeout(self, duration: core::time::Duration) -> crate::Timeout<Self> {
        crate::Timeout::local(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.submitted,
            stopping: request
                .reply
                .group
                .as_ref()
                .is_some_and(KillSwitch::is_stopping),
        })
    }

    pub async fn send(mut self) -> Reply<O> {
        if !poll_fn(|context| self.poll_submit(context)).await {
            stopped();
        }

        self.reply
    }

    pub fn try_send(mut self) -> Result<Reply<O>, TrySendError<Self>> {
        if self.submitted {
            return Ok(self.reply);
        }

        if self.message.is_none() {
            consumed();
        }

        if !self.handle.inner.open.get() {
            return Err(TrySendError::Closed(self));
        }

        let Some(message) = self.message.take() else {
            consumed()
        };

        match self.handle.inner.queue.try_send(message) {
            Ok(()) => Ok(self.reply),
            Err(embassy_sync::channel::TrySendError::Full(message)) => {
                self.message = Some(message);
                Err(TrySendError::Full(self))
            }
        }
    }

    fn poll_submit(&mut self, context: &mut Context<'_>) -> Poll<bool> {
        if self.submitted {
            return Poll::Ready(true);
        }

        if !self.handle.inner.open.get() {
            if self.handle.inner.lost() {
                return Poll::Pending;
            }
            return Poll::Ready(false);
        }

        self.closed.register(context);
        ready!(self.handle.inner.queue.poll_ready_to_send(context));

        let Some(message) = self.message.take() else {
            consumed()
        };

        match self.handle.inner.queue.try_send(message) {
            Ok(()) => {
                self.submitted = true;
                Poll::Ready(true)
            }
            Err(embassy_sync::channel::TrySendError::Full(message)) => {
                self.message = Some(message);
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
impl<S: 'static, const N: usize, E, O: 'static, Role> Future for Request<'_, S, N, E, O, Role> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if !ready!(this.poll_submit(context)) {
            stopped();
        }

        let output = Pin::new(&mut this.reply).poll(context);
        if output.is_ready() {
            this.submitted = false;
        }

        output
    }
}

impl<S: 'static, const N: usize, E, Role> Request<'_, S, N, E, (), Role> {
    pub async fn cast(self) {
        drop(self.send().await);
    }

    pub fn try_cast(self) -> Result<(), TrySendError<Self>> {
        self.try_send().map(drop)
    }
}

impl<O> Answer<O> {
    fn finish(&self, output: Result<O, CallError>) {
        let previous = {
            let mut result = self.result.borrow_mut();
            if !matches!(*result, AnswerState::Waiting(_)) {
                drop(result);
                drop(output);
                return;
            }

            core::mem::replace(&mut *result, AnswerState::Ready(output))
        };

        if let AnswerState::Waiting(Some(waker)) = previous {
            waker.wake();
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<O, CallError>> {
        let previous = {
            let mut result = self.result.borrow_mut();
            core::mem::replace(&mut *result, AnswerState::Consumed)
        };

        match previous {
            AnswerState::Ready(output) => Poll::Ready(output),
            AnswerState::Waiting(previous) => {
                *self.result.borrow_mut() = AnswerState::Waiting(Some(context.waker().clone()));
                drop(previous);
                Poll::Pending
            }
            _ => consumed(),
        }
    }
}

impl<S, F, I, O> Job<S> for Call<F, I, O>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O,
{
    fn run<'s>(&'s mut self, state: &'s mut S) -> LocalFuture<'s, ()> {
        Box::pin(async move {
            if let Some((function, input)) = self.function.take() {
                self.answer.finish(Ok(function(state, input).await));
            }
        })
    }
}
impl<F, I, O> Drop for Call<F, I, O> {
    fn drop(&mut self) {
        self.answer.finish(Err(if self.function.is_some() {
            CallError::Discarded
        } else {
            CallError::OutcomeUnknown
        }));
    }
}

impl<O> Reply<O> {
    /// Take a ready output once. A pending reply can still be awaited.
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken {
            return None;
        }
        match Pin::new(self).poll(&mut Context::from_waker(core::task::Waker::noop())) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        }
    }

    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::Timeout<&mut Self> {
        crate::Timeout::local(self, duration, |reply| crate::timeout::WaitStatus {
            admitted: true,
            stopping: reply.group.as_ref().is_some_and(KillSwitch::is_stopping),
        })
    }
}
impl<O> Future for Reply<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();
        if this.parked {
            return Poll::Pending;
        }
        match ready!(this.answer.poll(context)) {
            Ok(output) => {
                this.taken = true;
                Poll::Ready(output)
            }
            Err(_) => {
                if let Some(group) = &this.group {
                    group.fail(crate::ActorFailure {
                        actor: "actor".into(),
                        phase: "call".into(),
                        message: "actor stopped without an output".into(),
                    });
                    this.parked = true;
                    return Poll::Pending;
                }
                stopped()
            }
        }
    }
}

impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        let previous = self.answer.result.replace(AnswerState::Abandoned);
        drop(previous);
    }
}
