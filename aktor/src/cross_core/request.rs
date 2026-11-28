use super::*;
use crate::{AktorTimeout, message::consumed};
use core::{
    future::Future,
    ops::AsyncFnOnce,
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};

pub trait Body<S, I, O> {
    fn run(self: Box<Self>, state: &mut S, input: I) -> LocalFuture<'_, O>;
}

pub struct ReadBody<F>(pub F);
impl<S: 'static, I: 'static, O: 'static, F> Body<S, I, O> for ReadBody<F>
where
    F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
{
    fn run(self: Box<Self>, state: &mut S, input: I) -> LocalFuture<'_, O> {
        Box::pin(async move { self.0(state, input).await })
    }
}

pub struct WriteBody<F>(pub F);
impl<S: 'static, I: 'static, O: 'static, F> Body<S, I, O> for WriteBody<F>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
{
    fn run(self: Box<Self>, state: &mut S, input: I) -> LocalFuture<'_, O> {
        Box::pin(async move { self.0(state, input).await })
    }
}

pub enum Unsent<S, I, O> {
    // Opaque dispatch builds the call before it can apply the job's Send bounds.
    Call(Box<Call<S, I, O>>),
    Job(Box<dyn Job<S>>),
}

type Pending<S, I, O> = (I, Box<dyn Body<S, I, O> + Send>);

pub struct Call<S, I, O> {
    status: Arc<Status>,
    pending: Option<Pending<S, I, O>>,
    answer: Arc<Answer<O>>,
}
impl<S: 'static, I: Send + 'static, O: Send + 'static> Job<S> for Call<S, I, O> {
    fn run<'a>(
        &'a mut self,
        state: &'a mut S,
        hooks: &'a mut CallHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let Some((input, body)) = self.pending.take() else {
                consumed()
            };

            hooks.before(state, operation);

            let output = body.run(state, input).await;

            hooks.after(state, operation);
            self.answer.finish(Ok(output));
        })
    }
}
impl<S, I, O> Drop for Call<S, I, O> {
    fn drop(&mut self) {
        self.answer.finish(Err(crate::CallLoss(
            if self.pending.is_some() {
                AktorDeliveryOutcome::Discarded
            } else {
                AktorDeliveryOutcome::OutcomeUnknown
            },
            self.status.state.lock(|s| s.borrow().lost.clone()),
        )));
    }
}

impl<O> Answer<O> {
    pub fn finish(&self, output: Result<O, crate::CallLoss>) {
        let previous = self.result.lock(|result| {
            let mut result = result.borrow_mut();
            match *result {
                AnswerState::Waiting(_) => {
                    Ok(core::mem::replace(&mut *result, AnswerState::Ready(output)))
                }
                AnswerState::Ready(_) | AnswerState::Abandoned | AnswerState::Consumed => {
                    Err(output)
                }
            }
        });

        if let Ok(AnswerState::Waiting(Some(waker))) = previous {
            crate::local::wake(waker);
        }
    }
}

impl<'a, S, I, O, Role> Request<'a, S, I, O, Role> {
    pub fn new(
        handle: &'a Handle<S, Role>,
        operation: Operation,
        body: Box<dyn Body<S, I, O> + Send>,
        input: I,
    ) -> Self {
        let answer = Arc::new(Answer {
            result: Mutex::new(RefCell::new(AnswerState::Waiting(None))),
        });

        Self {
            handle,
            operation,
            pending: Some(Unsent::Call(Box::new(Call {
                pending: Some((input, body)),
                status: handle.inner.status.clone(),
                answer: answer.clone(),
            }))),
            changed: None,
            reply: Reply {
                answer,
                status: handle.inner.status.clone(),
                closed: None,
                taken: false,
            },
            submitted: false,
        }
    }
}
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> Request<'a, S, I, O, Role> {
    fn take_message(&mut self) -> Message<S> {
        let Some(pending) = self.pending.take() else {
            consumed()
        };
        let job = match pending {
            Unsent::Call(call) => call,
            Unsent::Job(job) => job,
        };

        Message {
            operation: self.operation,
            job,
        }
    }

    fn poll_admit(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if self.reply.taken {
            consumed();
        }
        if self.submitted {
            return Poll::Ready(true);
        }

        if self.changed.is_none() {
            let admitted = self.try_admit();
            if admitted.is_ready() {
                return admitted;
            }
        }

        self.changed
            .get_or_insert_with(|| Event::listen(&self.handle.inner.changed))
            .register(cx);
        self.try_admit()
    }

    fn try_admit(&mut self) -> Poll<bool> {
        let message = self.take_message();

        match self.handle.inner.commit(message) {
            Ok(()) => {
                self.submitted = true;
                self.handle.inner.changed.notify();
                self.changed = None;
                Poll::Ready(true)
            }
            Err(Refused::Full(message)) => {
                self.pending = Some(Unsent::Job(message.job));
                Poll::Pending
            }
            Err(Refused::Closed(message)) => {
                self.pending = Some(Unsent::Job(message.job));
                self.changed = None;
                Poll::Ready(false)
            }
        }
    }

    pub fn send(self) -> RequestSend<'a, S, I, O, Role> {
        RequestSend(Some(self))
    }

    pub fn timeout(self, duration: Duration) -> AktorTimeout<Self> {
        clock_timeout(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.submitted,
        })
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role> Future for Request<'_, S, I, O, Role> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !ready!(this.poll_admit(cx)) {
            return Poll::Ready(Err(this.reply.error(AktorDeliveryOutcome::NotAdmitted)));
        }
        Pin::new(&mut this.reply).poll(cx)
    }
}
impl<'a, S: 'static, I: Send + 'static, Role> Request<'a, S, I, (), Role> {
    pub fn cast(self) -> RequestCast<'a, S, I, Role> {
        RequestCast(self.send())
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RequestSend<'a, S, I, O, Role = ()>(Option<Request<'a, S, I, O, Role>>);
impl<S, I, O, Role> Unpin for RequestSend<'_, S, I, O, Role> {}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role> Future
    for RequestSend<'_, S, I, O, Role>
{
    type Output = Result<Reply<O>, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(request) = &mut this.0 else {
            consumed();
        };

        let admitted = ready!(request.poll_admit(cx));
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
pub struct RequestCast<'a, S, I, Role = ()>(RequestSend<'a, S, I, (), Role>);
impl<S, I, Role> Unpin for RequestCast<'_, S, I, Role> {}
impl<S: 'static, I: Send + 'static, Role> Future for RequestCast<'_, S, I, Role> {
    type Output = Result<(), crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx).map_ok(drop)
    }
}

impl<O> Reply<O> {
    fn error(&self, outcome: AktorDeliveryOutcome) -> crate::AktorDeliveryError {
        self.status.state.lock(|state| {
            let state = state.borrow();
            let cause = if outcome == AktorDeliveryOutcome::NotAdmitted {
                &state.refused
            } else {
                &state.lost
            };
            crate::Delivery::error(outcome, cause.as_ref())
        })
    }

    pub fn try_take(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        if self.taken {
            return None;
        }
        self.take(None)
    }

    fn take(&mut self, mut waker: Option<Waker>) -> Option<Result<O, crate::AktorDeliveryError>> {
        let result = self.answer.result.lock(|result| {
            let mut result = result.borrow_mut();

            if let AnswerState::Waiting(waiter) = &mut *result {
                if waker.is_some() {
                    core::mem::swap(waiter, &mut waker);
                }
                return None;
            }

            Some(core::mem::replace(&mut *result, AnswerState::Consumed))
        });

        let Some(result) = result else {
            if self.status.state.lock(|state| {
                state
                    .borrow()
                    .completed
                    .as_ref()
                    .is_some_and(Result::is_err)
            }) {
                self.taken = true;
                return Some(Err(self.error(AktorDeliveryOutcome::OutcomeUnknown)));
            }
            return None;
        };
        match result {
            AnswerState::Ready(output) => {
                self.taken = true;
                Some(match output {
                    Ok(value) => Ok(value),
                    Err(outcome) => Err(outcome.error(true)),
                })
            }
            _ => consumed(),
        }
    }

    pub fn timeout(&mut self, duration: Duration) -> AktorTimeout<&mut Self> {
        clock_timeout(self, duration, |_| crate::timeout::WaitStatus {
            admitted: true,
        })
    }
}
impl<O> Future for Reply<O> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.taken {
            consumed();
        }
        if let Some(output) = this.try_take() {
            return Poll::Ready(output);
        }
        this.closed
            .get_or_insert_with(|| Event::listen(&this.status.changed))
            .register(cx);
        match this.take(Some(cx.waker().clone())) {
            Some(output) => Poll::Ready(output),
            None => Poll::Pending,
        }
    }
}
impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        let previous = self
            .answer
            .result
            .lock(|result| core::mem::replace(&mut *result.borrow_mut(), AnswerState::Abandoned));

        drop(previous);
    }
}

fn clock_timeout<F>(
    future: F,
    duration: Duration,
    status: fn(&F) -> crate::timeout::WaitStatus,
) -> AktorTimeout<F> {
    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    {
        AktorTimeout::browser(future, duration, status)
    }
    #[cfg(not(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    )))]
    {
        AktorTimeout::local(future, duration, status)
    }
}
