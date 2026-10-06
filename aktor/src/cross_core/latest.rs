use super::*;
use crate::{
    dispatch::OwnedState,
    latest::{SendLatest, Session, TypedSession},
    message::stopped,
};
use core::{
    future::{Future, poll_fn},
    ops::AsyncFnOnce,
    pin::Pin,
    task::{Context, Poll},
};

pub struct LatestSender<I> {
    input: Arc<dyn Input<I>>,
}
impl<I> Clone for LatestSender<I> {
    fn clone(&self) -> Self {
        self.input.clone_sender();
        Self {
            input: self.input.clone(),
        }
    }
}
impl<I> Drop for LatestSender<I> {
    fn drop(&mut self) {
        self.input.close_sender();
    }
}
impl<I> SendLatest<I> for LatestSender<I> {
    fn send(&self, input: I) {
        self.input.send(input);
    }
}
impl<I> LatestSender<I> {
    pub fn send(&self, input: I) {
        self.input.send(input);
    }
}

pub struct LatestResults<O> {
    output: Box<dyn Output<O>>,
    status: Arc<Status>,
    changed: Listener,
    completed: Listener,
}
impl<O> LatestResults<O> {
    pub async fn next(&mut self) -> Option<O> {
        poll_fn(|cx| self.poll(cx)).await
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<O>> {
        self.changed.register(cx);
        self.completed.register(cx);

        let state = self.status.state.lock(|state| {
            let state = state.borrow();
            (state.closing, state.completed.clone())
        });

        match self.output.poll(state.0, state.1.as_ref()) {
            Ok(output) => output,
            Err(()) if self.status.managed() => Poll::Pending,
            Err(()) => stopped(),
        }
    }
}
impl<O> futures_core::Stream for LatestResults<O> {
    type Item = O;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<O>> {
        self.get_mut().poll(cx)
    }
}
impl<O> Drop for LatestResults<O> {
    fn drop(&mut self) {
        self.output.close_receiver();
    }
}

trait Input<I>: Send + Sync {
    fn send(&self, input: I);
    fn clone_sender(&self);
    fn close_sender(&self);
}
trait Output<O>: Send + Sync {
    fn poll(
        &self,
        closing: bool,
        completed: Option<&Result<(), AktorError>>,
    ) -> Result<Poll<Option<O>>, ()>;
    fn close_receiver(&self);
}

struct Shared<S, I, O, F, Role> {
    handle: Handle<S, Role>,
    slot: Lock<Slot<I, O>>,
    function: Lock<Option<F>>,
    changed: Arc<Event>,
    operation: Operation,
    this: Weak<Self>,
}

struct Slot<I, O> {
    revision: u64,
    pending: Option<I>,
    output: Option<O>,
    scheduled: bool,
    running: bool,
    senders: usize,
    receiver: bool,
    failed: bool,
}

struct LatestJob<S, I, O, F, Role> {
    shared: Arc<Shared<S, I, O, F, Role>>,
    finished: bool,
}
impl<S, I, O, F, Role> Drop for LatestJob<S, I, O, F, Role> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        let discarded = self.shared.slot.lock(|slot| {
            let mut slot = slot.borrow_mut();

            slot.scheduled = false;
            slot.running = false;
            slot.failed = true;
            (slot.pending.take(), slot.output.take())
        });

        drop(discarded);
        self.shared.changed.notify();
    }
}

impl<S: 'static, I: Send + 'static, O: Send + 'static, F, Role: 'static> Shared<S, I, O, F, Role>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
{
    fn schedule(&self, slot: &mut Slot<I, O>) -> Option<Result<(), Message<S>>> {
        if slot.scheduled || slot.running || !slot.receiver || slot.failed || slot.pending.is_none()
        {
            return None;
        }

        let this = self.this.upgrade()?;
        let committed = self.handle.inner.commit(
            Message {
                operation: self.operation,
                job: Box::new(LatestJob {
                    shared: this,
                    finished: false,
                }),
            },
            true,
        );

        slot.scheduled = committed.is_ok();
        slot.failed |= committed.is_err();
        Some(committed)
    }

    fn publish(&self, committed: Option<Result<(), Message<S>>>) {
        match committed {
            Some(Ok(())) => self.handle.inner.changed.notify(),
            Some(Err(message)) => drop(message),
            None => {}
        }

        self.changed.notify();
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, F, Role: 'static> Input<I>
    for Arc<Shared<S, I, O, F, Role>>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
{
    fn send(&self, input: I) {
        let mut input = Some(input);
        let transition = self.slot.lock(|slot| {
            let mut slot = slot.borrow_mut();

            if !slot.receiver
                || slot.failed
                || !self.handle.inner.queue.lock(|queue| queue.borrow().open)
            {
                return None;
            }

            slot.revision = slot.revision.wrapping_add(1);

            let old = (
                core::mem::replace(&mut slot.pending, input.take()),
                slot.output.take(),
            );

            Some((old, self.schedule(&mut slot)))
        });

        drop(input);

        if let Some((old, committed)) = transition {
            self.publish(committed);
            drop(old);
        }
    }

    fn clone_sender(&self) {
        self.slot.lock(|slot| slot.borrow_mut().senders += 1);
    }

    fn close_sender(&self) {
        self.slot.lock(|slot| slot.borrow_mut().senders -= 1);
        self.changed.notify();
    }
}
impl<S, I: Send, O: Send, F: Send, Role> Output<O> for Arc<Shared<S, I, O, F, Role>> {
    fn poll(
        &self,
        closing: bool,
        completed: Option<&Result<(), AktorError>>,
    ) -> Result<Poll<Option<O>>, ()> {
        self.slot.lock(|slot| {
            let mut slot = slot.borrow_mut();

            if !slot.failed
                && let Some(output) = slot.output.take()
            {
                return Ok(Poll::Ready(Some(output)));
            }

            if completed.is_some_and(Result::is_err) || (slot.failed && completed.is_some()) {
                return Err(());
            }

            if !slot.failed
                && (closing || slot.senders == 0)
                && slot.pending.is_none()
                && !slot.scheduled
                && !slot.running
            {
                return Ok(Poll::Ready(None));
            }

            Ok(Poll::Pending)
        })
    }

    fn close_receiver(&self) {
        let discarded = self.slot.lock(|slot| {
            let mut slot = slot.borrow_mut();
            slot.receiver = false;
            (slot.pending.take(), slot.output.take())
        });

        drop(discarded);
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, F, Role: 'static> Job<S>
    for LatestJob<S, I, O, F, Role>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
{
    fn run<'a>(
        &'a mut self,
        state: &'a mut S,
        hooks: &'a mut AktorHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let request = self.shared.slot.lock(|slot| {
                let mut slot = slot.borrow_mut();

                slot.scheduled = false;

                if !slot.receiver {
                    return None;
                }

                let input = slot.pending.take()?;

                slot.running = true;
                Some((slot.revision, input))
            });

            let Some((revision, input)) = request else {
                self.finished = true;
                return;
            };
            let function = self
                .shared
                .function
                .lock(|function| function.borrow_mut().take());
            let Some(function) = function else {
                crate::message::consumed()
            };
            let next = function.clone();

            self.shared
                .function
                .lock(|slot| *slot.borrow_mut() = Some(function));
            hooks.before(state, operation);

            let output = next(state, input).await;

            hooks.after(state, operation);
            self.finished = true;

            let (discarded, committed) = self.shared.slot.lock(|slot| {
                let mut slot = slot.borrow_mut();

                slot.running = false;

                let discarded = if slot.receiver && slot.revision == revision {
                    slot.output.replace(output)
                } else {
                    Some(output)
                };

                (discarded, self.shared.schedule(&mut slot))
            });

            self.shared.publish(committed);
            drop(discarded);
        })
    }
}

impl<S: 'static, I: Send + 'static, O: Send + 'static, Role: 'static> Session<S, I, O, Role>
    for &Handle<S, Role>
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F>(self, operation: Operation, function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        let shared = Arc::new_cyclic(|this| Shared {
            handle: self.new_handle(),
            slot: Mutex::new(RefCell::new(Slot {
                revision: 0,
                pending: None,
                output: None,
                scheduled: false,
                running: false,
                senders: 1,
                receiver: true,
                failed: false,
            })),
            function: Mutex::new(RefCell::new(Some(function))),
            changed: Arc::new(Event::default()),
            operation,
            this: this.clone(),
        });

        (
            LatestSender {
                input: Arc::from(Box::new(shared.clone()) as Box<dyn Input<I>>),
            },
            LatestResults {
                changed: Event::listen(&shared.changed),
                completed: Event::listen(&shared.handle.inner.status.changed),
                status: shared.handle.inner.status.clone(),
                output: Box::new(shared),
            },
        )
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role: 'static, Wire>
    TypedSession<S, I, O, OwnedState<S>, Role, Wire> for &Handle<S, Role>
{
    type Sender<Fut> = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F, Fut>(
        self,
        operation: Operation,
        function: F,
        _: fn(OwnedState<S>, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: Future<Output = (OwnedState<S>, O)>,
    {
        Session::session(self, operation, function)
    }
}
