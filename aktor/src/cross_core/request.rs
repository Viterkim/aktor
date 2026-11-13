use super::*;
use crate::{
    AktorTimeout,
    message::{consumed, stopped},
};
use core::{
    future::{Future, poll_fn},
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
        self.answer.finish(Err(if self.pending.is_some() {
            CallError::Discarded
        } else {
            CallError::OutcomeUnknown
        }));
    }
}

impl<O> Answer<O> {
    pub fn finish(&self, output: Result<O, CallError>) {
        let previous = self.result.lock(|result| {
            let mut result = result.borrow_mut();
            if matches!(*result, AnswerState::Waiting(_)) {
                Ok(core::mem::replace(&mut *result, AnswerState::Ready(output)))
            } else {
                Err(output)
            }
        });

        if let Ok(AnswerState::Waiting(Some(waker))) = previous {
            waker.wake();
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
                answer: answer.clone(),
            }))),
            changed: None,
            reply: Reply {
                answer,
                status: handle.inner.status.clone(),
                parked: false,
                taken: false,
            },
            submitted: false,
            parked: false,
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

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.reply.taken {
            consumed();
        }
        if self.parked {
            return Poll::Pending;
        }

        if self.submitted {
            return Poll::Ready(());
        }

        if self.changed.is_none() {
            let admitted = self.try_submit();
            if admitted.is_ready() || self.parked {
                return admitted;
            }
        }

        self.changed
            .get_or_insert_with(|| Event::listen(&self.handle.inner.changed))
            .register(cx);
        // The owner may have freed a slot before we registered.
        self.try_submit()
    }

    fn try_submit(&mut self) -> Poll<()> {
        let message = self.take_message();

        match self.handle.inner.commit(message) {
            Ok(()) => {
                self.submitted = true;
                self.handle.inner.changed.notify();
                self.changed = None;
                Poll::Ready(())
            }
            Err(Refused::Full(message)) => {
                self.pending = Some(Unsent::Job(message.job));
                Poll::Pending
            }
            Err(Refused::Closed(message)) => {
                self.pending = Some(Unsent::Job(message.job));
                self.changed = None;

                if self.handle.inner.status.managed() {
                    self.parked = true;
                    return Poll::Pending;
                }

                stopped();
            }
        }
    }

    pub async fn send(mut self) -> Reply<O> {
        if self.reply.taken {
            consumed();
        }

        poll_fn(|cx| self.poll_submit(cx)).await;
        self.reply
    }

    pub fn timeout(self, duration: Duration) -> AktorTimeout<Self> {
        clock_timeout(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.submitted,
        })
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role> Future for Request<'_, S, I, O, Role> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();
        ready!(this.poll_submit(cx));
        Pin::new(&mut this.reply).poll(cx)
    }
}
impl<S: 'static, I: Send + 'static, Role> Request<'_, S, I, (), Role> {
    pub async fn cast(self) {
        drop(self.send().await);
    }
}

impl<O> Reply<O> {
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken || self.parked {
            return None;
        }

        self.take(None)
    }

    fn take(&mut self, mut waker: Option<Waker>) -> Option<O> {
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

        match result? {
            AnswerState::Ready(Ok(output)) => {
                self.taken = true;
                Some(output)
            }
            AnswerState::Ready(Err(_)) => {
                if self.status.managed() {
                    self.parked = true;
                    None
                } else {
                    stopped()
                }
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
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if this.parked {
            return Poll::Pending;
        }

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
