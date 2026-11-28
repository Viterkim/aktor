use super::*;
use crate::message::consumed;
use core::{
    future::poll_fn,
    ops::AsyncFnOnce,
    pin::Pin,
    task::{Context, ready},
};

impl<'a, S: 'static, O: 'static, Role, Clock> Request<'a, S, O, Role, Clock> {
    pub fn new<F, I>(
        handle: &'a Handle<S, Role, Clock>,
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
                operation,
                job: Box::new(Call {
                    function: Some((function, input)),
                    completion: handle.inner.completion.clone(),
                    answer: answer.clone(),
                }),
            }),
            reply: Reply {
                clock: PhantomData,
                taken: false,
                answer,
                completion: handle.inner.completion.clone(),
            },
            space: None,
            submitted: false,
        }
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "embassy",
        feature = "browser_local"
    ))]
    pub fn timeout(self, duration: core::time::Duration) -> crate::AktorTimeout<Self>
    where
        Clock: clock::AktorClock,
    {
        Clock::timeout(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.submitted,
        })
    }

    pub fn send(self) -> RequestSend<'a, S, O, Role, Clock> {
        RequestSend(Some(self))
    }

    fn poll_admit(&mut self, context: &mut Context<'_>) -> Poll<bool> {
        if self.reply.taken {
            consumed();
        }

        if self.submitted {
            return Poll::Ready(true);
        }

        if self.space.is_none() {
            let admitted = self.try_admit();
            if admitted.is_ready() {
                return admitted;
            }
        }

        self.space
            .get_or_insert_with(|| self.handle.inner.space.listen())
            .register(context);
        self.try_admit()
    }

    fn try_admit(&mut self) -> Poll<bool> {
        if !self.handle.inner.open.get() {
            self.space = None;
            return Poll::Ready(false);
        }

        let Some(message) = self.message.take() else {
            consumed()
        };

        match self.handle.inner.enqueue(message) {
            Ok(()) => {
                self.submitted = true;
                self.space = None;
                Poll::Ready(true)
            }
            Err(message) => {
                self.message = Some(message);
                Poll::Pending
            }
        }
    }
}
impl<S: 'static, O: 'static, Role, Clock> Future for Request<'_, S, O, Role, Clock> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if !ready!(this.poll_admit(context)) {
            return Poll::Ready(Err(this.reply.error(AktorDeliveryOutcome::NotAdmitted)));
        }
        let output = Pin::new(&mut this.reply).poll(context);

        if output.is_ready() {
            this.submitted = false;
        }

        output
    }
}
impl<'a, S: 'static, Role, Clock> Request<'a, S, (), Role, Clock> {
    #[doc(hidden)]
    pub async fn run_interval(mut self) -> bool {
        let admitted = poll_fn(|cx| self.poll_admit(cx)).await;

        if admitted {
            return self.reply.await.is_ok();
        }

        admitted
    }

    pub fn cast(self) -> RequestCast<'a, S, Role, Clock> {
        RequestCast(self.send())
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RequestSend<'a, S, O, Role = (), Clock = ()>(Option<Request<'a, S, O, Role, Clock>>);
impl<S, O, Role, Clock> Unpin for RequestSend<'_, S, O, Role, Clock> {}
impl<S: 'static, O: 'static, Role, Clock> Future for RequestSend<'_, S, O, Role, Clock> {
    type Output = Result<Reply<O, Clock>, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(request) = &mut this.0 else {
            consumed();
        };

        let admitted = ready!(request.poll_admit(context));
        let Some(request) = this.0.take() else {
            consumed();
        };

        if admitted {
            Poll::Ready(Ok(request.reply))
        } else {
            Poll::Ready(Err(request.reply.error(AktorDeliveryOutcome::NotAdmitted)))
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RequestCast<'a, S, Role = (), Clock = ()>(RequestSend<'a, S, (), Role, Clock>);
impl<S, Role, Clock> Unpin for RequestCast<'_, S, Role, Clock> {}
impl<S: 'static, Role, Clock> Future for RequestCast<'_, S, Role, Clock> {
    type Output = Result<(), crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context).map_ok(drop)
    }
}

impl<O> Answer<O> {
    fn try_take(&self) -> Option<Result<O, crate::CallLoss>> {
        let mut result = self.result.borrow_mut();

        match *result {
            AnswerState::Waiting(_) => return None,
            AnswerState::Ready(_) | AnswerState::Abandoned | AnswerState::Consumed => {}
        }

        let previous = core::mem::replace(&mut *result, AnswerState::Consumed);

        drop(result);

        match previous {
            AnswerState::Ready(output) => Some(output),
            AnswerState::Waiting(_) | AnswerState::Abandoned | AnswerState::Consumed => consumed(),
        }
    }

    fn finish(&self, output: Result<O, crate::CallLoss>) {
        let previous = {
            let mut result = self.result.borrow_mut();

            match *result {
                AnswerState::Waiting(_) => {}
                AnswerState::Ready(_) | AnswerState::Abandoned | AnswerState::Consumed => {
                    drop(result);
                    drop(output);
                    return;
                }
            }

            core::mem::replace(&mut *result, AnswerState::Ready(output))
        };

        if let AnswerState::Waiting(Some(waker)) = previous {
            crate::local::wake(waker);
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<O, crate::CallLoss>> {
        if let Some(output) = self.try_take() {
            return Poll::Ready(output);
        }

        let replacement = context.waker().clone();
        let mut result = self.result.borrow_mut();

        if let AnswerState::Waiting(waiter) = &mut *result {
            let old = waiter.replace(replacement);
            drop(result);
            drop(old);
            return Poll::Pending;
        }

        // Cloning the waker can run code that completes the call.
        drop(result);
        self.try_take().map_or_else(|| consumed(), Poll::Ready)
    }
}

impl<S, F, I, O> Job<S> for Call<F, I, O>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O,
{
    fn run<'s>(
        &'s mut self,
        state: &'s mut S,
        hooks: &'s mut hooks::CallHooks<S>,
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
        self.answer.finish(Err(crate::CallLoss(
            if self.function.is_some() {
                AktorDeliveryOutcome::Discarded
            } else {
                AktorDeliveryOutcome::OutcomeUnknown
            },
            self.completion.lost.get().cloned(),
        )));
    }
}

impl<O, Clock> Reply<O, Clock> {
    fn error(&self, outcome: AktorDeliveryOutcome) -> crate::AktorDeliveryError {
        crate::Delivery::error(outcome, self.completion.refused.get())
    }

    pub fn try_take(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        if self.taken {
            return None;
        }
        let output = self.answer.try_take()?;
        self.taken = true;
        Some(match output {
            Ok(value) => Ok(value),
            Err(outcome) => Err(outcome.error(true)),
        })
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "embassy",
        feature = "browser_local"
    ))]
    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::AktorTimeout<&mut Self>
    where
        Clock: clock::AktorClock,
    {
        Clock::timeout(self, duration, |_| crate::timeout::WaitStatus {
            admitted: true,
        })
    }
}
impl<O, Clock> Future for Reply<O, Clock> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.taken {
            consumed();
        }
        let result = ready!(this.answer.poll(context));
        this.taken = true;
        Poll::Ready(match result {
            Ok(value) => Ok(value),
            Err(outcome) => Err(outcome.error(true)),
        })
    }
}
impl<O, Clock> Drop for Reply<O, Clock> {
    fn drop(&mut self) {
        let previous = self.answer.result.replace(AnswerState::Abandoned);
        drop(previous);
    }
}
