use super::*;
use crate::message::{consumed, stopped};
use core::{
    future::poll_fn,
    ops::AsyncFnOnce,
    pin::Pin,
    task::{Context, ready},
};

impl<'a, S: 'static, const N: usize, E, O: 'static, Role, Clock>
    Request<'a, S, N, E, O, Role, Clock>
{
    pub fn new<F, I>(
        handle: &'a Handle<S, N, E, Role, Clock>,
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
                clock: PhantomData,
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

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "embassy",
        feature = "browser_local"
    ))]
    pub fn timeout(self, duration: core::time::Duration) -> crate::Timeout<Self>
    where
        Clock: clock::AktorClock,
    {
        Clock::timeout(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.submitted,
            stopping: request
                .reply
                .group
                .as_ref()
                .is_some_and(StopSignal::is_stopping),
        })
    }

    pub async fn send(mut self) -> Reply<O, Clock> {
        if !poll_fn(|context| self.poll_submit(context)).await {
            stopped();
        }

        self.reply
    }

    fn poll_submit(&mut self, context: &mut Context<'_>) -> Poll<bool> {
        let result = self.poll_admit(context);

        if matches!(result, Poll::Ready(false)) && self.handle.inner.rejected() {
            Poll::Pending
        } else {
            result
        }
    }

    fn poll_admit(&mut self, context: &mut Context<'_>) -> Poll<bool> {
        if self.reply.taken {
            consumed();
        }

        if self.submitted {
            return Poll::Ready(true);
        }

        self.closed.register(context);

        if !self.handle.inner.open.get() {
            return Poll::Ready(false);
        }

        let Some(message) = self.message.take() else {
            consumed()
        };

        match self.handle.inner.enqueue(message) {
            Ok(()) => {
                self.submitted = true;
                Poll::Ready(true)
            }
            Err(message) => {
                self.message = Some(message);
                Poll::Pending
            }
        }
    }
}
impl<S: 'static, const N: usize, E, O: 'static, Role, Clock> Future
    for Request<'_, S, N, E, O, Role, Clock>
{
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
impl<S: 'static, const N: usize, E, Role, Clock> Request<'_, S, N, E, (), Role, Clock> {
    #[doc(hidden)]
    pub async fn run_interval(mut self) -> bool {
        let admitted = poll_fn(|cx| self.poll_admit(cx)).await;

        if admitted {
            self.reply.await;
        }

        admitted
    }

    pub async fn cast(self) {
        drop(self.send().await);
    }
}

impl<O> Answer<O> {
    fn try_take(&self) -> Option<Result<O, CallError>> {
        let mut result = self.result.borrow_mut();

        if matches!(*result, AnswerState::Waiting(_)) {
            return None;
        }

        let previous = core::mem::replace(&mut *result, AnswerState::Consumed);

        drop(result);

        match previous {
            AnswerState::Ready(output) => Some(output),
            _ => consumed(),
        }
    }

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
        let replacement = context.waker().clone();
        let previous = {
            let mut result = self.result.borrow_mut();
            core::mem::replace(&mut *result, AnswerState::Consumed)
        };

        match previous {
            AnswerState::Ready(output) => Poll::Ready(output),
            AnswerState::Waiting(previous) => {
                *self.result.borrow_mut() = AnswerState::Waiting(Some(replacement));
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
    fn run<'s>(
        &'s mut self,
        state: &'s mut S,
        hooks: &'s mut hooks::AktorHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'s, ()> {
        Box::pin(async move {
            if let Some((function, input)) = self.function.take() {
                hooks.before(state, operation);

                let output = function(state, input).await;

                hooks.after(state, operation);
                self.answer.finish(Ok(output));
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

impl<O, Clock> Reply<O, Clock> {
    /// Take a ready output once. A pending reply can still be awaited.
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken || self.parked {
            return None;
        }

        match self.answer.try_take()? {
            Ok(output) => {
                self.taken = true;
                Some(output)
            }
            Err(_) => self.lost(),
        }
    }

    fn lost(&mut self) -> Option<O> {
        if let Some(group) = &self.group {
            group.fail(crate::ActorFailure {
                kind: None,
                actor: "actor".into(),
                phase: "call".into(),
                message: "actor stopped without an output".into(),
            });

            self.parked = true;
            None
        } else {
            stopped()
        }
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "embassy",
        feature = "browser_local"
    ))]
    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::Timeout<&mut Self>
    where
        Clock: clock::AktorClock,
    {
        Clock::timeout(self, duration, |reply| crate::timeout::WaitStatus {
            admitted: true,
            stopping: reply.group.as_ref().is_some_and(StopSignal::is_stopping),
        })
    }
}
impl<O, Clock> Future for Reply<O, Clock> {
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
                this.lost();
                Poll::Pending
            }
        }
    }
}
impl<O, Clock> Drop for Reply<O, Clock> {
    fn drop(&mut self) {
        let previous = self.answer.result.replace(AnswerState::Abandoned);
        drop(previous);
    }
}
