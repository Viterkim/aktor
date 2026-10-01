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
            reply: Reply { answer },
            closed: handle.inner.closed.listen(),
            submitted: false,
        }
    }

    pub fn checked(self) -> CheckedRequest<'a, S, N, E, O, Role> {
        CheckedRequest(self)
    }

    pub async fn send(mut self) -> Reply<O> {
        if !poll_fn(|context| self.poll_submit(context)).await {
            stopped();
        }

        self.reply
    }

    pub async fn checked_send(mut self) -> Result<CheckedReply<O>, CallError> {
        if !poll_fn(|context| self.poll_submit(context)).await {
            return Err(CallError::NotAdmitted);
        }

        Ok(self.reply.checked())
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

        let output = ready!(this.reply.answer.poll(context));
        this.submitted = false;
        match output {
            Ok(output) => Poll::Ready(output),
            Err(_) => stopped(),
        }
    }
}

impl<S: 'static, const N: usize, E, O: 'static, Role> Future
    for CheckedRequest<'_, S, N, E, O, Role>
{
    type Output = Result<O, CallError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut self.get_mut().0;

        if !ready!(this.poll_submit(context)) {
            return Poll::Ready(Err(CallError::NotAdmitted));
        }

        let output = ready!(this.reply.answer.poll(context));
        this.submitted = false;
        Poll::Ready(output)
    }
}

impl<S: 'static, const N: usize, E, Role> Request<'_, S, N, E, (), Role> {
    pub async fn cast(self) {
        drop(self.send().await);
    }

    pub async fn checked_cast(self) -> Result<(), CallError> {
        drop(self.checked_send().await?);
        Ok(())
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
    pub fn checked(self) -> CheckedReply<O> {
        CheckedReply(self)
    }
}
impl<O> Future for Reply<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<O> {
        match ready!(self.answer.poll(context)) {
            Ok(output) => Poll::Ready(output),
            Err(_) => stopped(),
        }
    }
}

impl<O> Future for CheckedReply<O> {
    type Output = Result<O, CallError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.answer.poll(context)
    }
}

impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        let previous = self.answer.result.replace(AnswerState::Abandoned);
        drop(previous);
    }
}
