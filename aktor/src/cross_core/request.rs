use super::*;
use crate::{
    Timeout,
    message::{TrySendError, consumed, stopped},
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

pub type Pending<S, I, O> = Box<(I, Box<dyn Body<S, I, O> + Send>)>;

pub enum Unsent<S, I, O> {
    Body(Pending<S, I, O>),
    Job(Box<dyn Job<S>>),
}

struct Call<S, I, O> {
    pending: Option<Pending<S, I, O>>,
    answer: Arc<Answer<O>>,
}
impl<S: 'static, I: Send + 'static, O: Send + 'static> Job<S> for Call<S, I, O> {
    fn run<'a>(
        &'a mut self,
        state: &'a mut S,
        hooks: &'a mut AktorHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let Some(pending) = self.pending.take() else {
                consumed()
            };
            let (input, body) = *pending;

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
        let mut output = Some(output);
        let previous = self.result.lock(|result| {
            let mut result = result.borrow_mut();

            if matches!(*result, AnswerState::Waiting) {
                output
                    .take()
                    .map(|output| core::mem::replace(&mut *result, AnswerState::Ready(output)))
            } else {
                None
            }
        });

        drop(previous);
        drop(output);
        self.changed.notify();
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
            result: Mutex::new(RefCell::new(AnswerState::Waiting)),
            changed: Arc::new(Event::default()),
        });

        Self {
            handle,
            operation,
            pending: Some(Unsent::Body(Box::new((input, body)))),
            changed: Event::listen(&handle.inner.changed),
            reply: Reply {
                changed: Event::listen(&answer.changed),
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
            Unsent::Body(pending) => Box::new(Call {
                pending: Some(pending),
                answer: self.reply.answer.clone(),
            }) as Box<dyn Job<S>>,
            Unsent::Job(job) => job,
        };

        Message {
            operation: self.operation,
            job,
        }
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.parked {
            return Poll::Pending;
        }

        if self.submitted {
            return Poll::Ready(());
        }

        self.changed.register(cx);

        let message = self.take_message();

        match self.handle.inner.commit(message, false) {
            Ok(()) => {
                self.submitted = true;
                self.handle.inner.changed.notify();
                Poll::Ready(())
            }
            Err(message) => {
                self.pending = Some(Unsent::Job(message.job));

                let open = self.handle.inner.queue.lock(|queue| queue.borrow().open);

                if !open {
                    if self.handle.inner.status.managed() {
                        self.parked = true;
                        return Poll::Pending;
                    }

                    stopped();
                }

                Poll::Pending
            }
        }
    }

    pub async fn send(mut self) -> Reply<O> {
        poll_fn(|cx| self.poll_submit(cx)).await;
        self.reply
    }

    pub fn try_send(mut self) -> Result<Reply<O>, TrySendError<Self>> {
        if self.submitted {
            return Ok(self.reply);
        }

        let message = self.take_message();

        match self.handle.inner.commit(message, false) {
            Ok(()) => {
                self.handle.inner.changed.notify();
                Ok(self.reply)
            }
            Err(message) => {
                self.pending = Some(Unsent::Job(message.job));

                if self.handle.inner.queue.lock(|queue| queue.borrow().open) {
                    Err(TrySendError::Full(self))
                } else {
                    Err(TrySendError::Closed(self))
                }
            }
        }
    }

    pub fn timeout(self, duration: Duration) -> Timeout<Self> {
        Timeout::local(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.submitted,
            stopping: request.reply.status.stopping(),
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

    pub fn try_cast(self) -> Result<(), TrySendError<Self>> {
        self.try_send().map(drop)
    }
}

impl<O> Reply<O> {
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken || self.parked {
            return None;
        }

        self.take()
    }

    fn take(&mut self) -> Option<O> {
        let result = self.answer.result.lock(|result| {
            let mut result = result.borrow_mut();

            if matches!(*result, AnswerState::Waiting) {
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

    pub fn timeout(&mut self, duration: Duration) -> Timeout<&mut Self> {
        Timeout::local(self, duration, |reply| crate::timeout::WaitStatus {
            admitted: true,
            stopping: reply.status.stopping(),
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

        this.changed.register(cx);

        match this.take() {
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
