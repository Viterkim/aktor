use super::*;
use crate::message::stopped;
use alloc::vec::Vec;
use core::pin::Pin;
use core::task::Context;

use crate::latest::{SendLatest, Session};
use core::{future::poll_fn, ops::AsyncFnOnce};

pub struct LatestSender<I> {
    input: Rc<dyn Input<I>>,
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

pub struct LatestResults<O> {
    output: Rc<dyn Output<O>>,
}
impl<O> LatestResults<O> {
    pub async fn next(&mut self) -> Option<O> {
        poll_fn(|cx| self.output.poll(cx)).await
    }
}
impl<O> futures_core::Stream for LatestResults<O> {
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<O>> {
        self.output.poll(cx)
    }
}
impl<O> Drop for LatestResults<O> {
    fn drop(&mut self) {
        self.output.close_receiver();
    }
}

trait Input<I> {
    fn send(&self, input: I);
    fn clone_sender(&self);
    fn close_sender(&self);
}

trait Output<O> {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>>;
    fn close_receiver(&self);
}

struct Shared<S, I, O, F, Role, const N: usize, E> {
    handle: Handle<S, N, E, Role>,
    state: RefCell<Slot<I, O>>,
    function: RefCell<F>,
    operation: crate::operation::Operation,
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
    wake: Option<Waker>,
}

struct LatestJob<S, I, O, F, Role, const N: usize, E>(Rc<Shared<S, I, O, F, Role, N, E>>);

impl<S: 'static, I: 'static, O: 'static, Role, F, const N: usize, E: 'static>
    Shared<S, I, O, F, Role, N, E>
where
    Role: 'static,
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + 'static,
{
    fn schedule(&self, slot: &mut Slot<I, O>) {
        if slot.scheduled || slot.running || !slot.receiver || slot.pending.is_none() {
            return;
        }

        if let Some(this) = self.this.upgrade() {
            slot.scheduled = true;
            self.handle.inner.service(Message {
                _operation: self.operation,
                job: Box::new(LatestJob(this)),
            });
        }
    }
}
impl<S: 'static, I: 'static, O: 'static, Role: 'static, F, const N: usize, E: 'static> Input<I>
    for Shared<S, I, O, F, Role, N, E>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + 'static,
{
    fn send(&self, input: I) {
        let mut slot = self.state.borrow_mut();

        if !slot.receiver || slot.failed || !self.handle.inner.open.get() {
            return;
        }

        slot.revision = slot.revision.wrapping_add(1);
        let previous = slot.pending.replace(input);
        let unread = slot.output.take();
        self.schedule(&mut slot);
        drop(slot);
        drop(previous);
        drop(unread);
    }

    fn clone_sender(&self) {
        self.state.borrow_mut().senders += 1;
    }

    fn close_sender(&self) {
        let mut slot = self.state.borrow_mut();
        slot.senders = slot.senders.saturating_sub(1);
        let wake = slot.wake.take();

        drop(slot);

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<S, I, O, F, Role, const N: usize, E> Output<O> for Shared<S, I, O, F, Role, N, E> {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>> {
        let mut slot = self.state.borrow_mut();

        if self
            .handle
            .inner
            .completion
            .result
            .borrow()
            .as_ref()
            .is_some_and(Result::is_err)
        {
            if self.handle.inner.lost() {
                return Poll::Pending;
            }
            stopped();
        }

        if let Some(output) = slot.output.take() {
            return Poll::Ready(Some(output));
        }

        if (slot.senders == 0 || !self.handle.inner.open.get())
            && slot.pending.is_none()
            && !slot.running
            && !slot.scheduled
        {
            return Poll::Ready(None);
        }

        slot.wake = Some(cx.waker().clone());
        Poll::Pending
    }

    fn close_receiver(&self) {
        let mut slot = self.state.borrow_mut();
        slot.receiver = false;
        let pending = slot.pending.take();
        let output = slot.output.take();
        let wake = slot.wake.take();

        drop(slot);
        drop(pending);
        drop(output);
        drop(wake);
    }
}
impl<S: 'static, I: 'static, O: 'static, Role: 'static, F, const N: usize, E: 'static> Job<S>
    for LatestJob<S, I, O, F, Role, N, E>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + 'static,
{
    fn run<'a>(&'a mut self, state: &'a mut S) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let request = {
                let mut slot = self.0.state.borrow_mut();
                slot.scheduled = false;
                if !slot.receiver {
                    return;
                }
                let Some(input) = slot.pending.take() else {
                    return;
                };
                slot.running = true;
                (slot.revision, input)
            };

            let function = self.0.function.borrow().clone();
            let output = function(state, request.1).await;

            let mut slot = self.0.state.borrow_mut();
            slot.running = false;
            if slot.receiver && slot.revision == request.0 {
                slot.output = Some(output);
            }
            self.0.schedule(&mut slot);
            let wake = slot.wake.take();
            drop(slot);
            if let Some(wake) = wake {
                wake.wake();
            }
        })
    }
}
impl<S: 'static, I: 'static, O: 'static, Role: 'static, const N: usize, E: 'static>
    Session<S, I, O, Role> for &Handle<S, N, E, Role>
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F>(
        self,
        operation: crate::operation::Operation,
        function: F,
    ) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        let shared = Rc::new_cyclic(|this| Shared {
            handle: self.new_handle(),
            state: RefCell::new(Slot {
                revision: 0,
                pending: None,
                output: None,
                scheduled: false,
                running: false,
                senders: 1,
                receiver: true,
                failed: false,
                wake: None,
            }),
            function: RefCell::new(function),
            operation,
            this: this.clone(),
        });
        let weak = Rc::downgrade(&shared);
        let callbacks = self.inner.sessions.borrow().clone();
        let expired: Vec<_> = callbacks.into_iter().filter(|live| !live()).collect();
        self.inner
            .sessions
            .borrow_mut()
            .retain(|live| !expired.iter().any(|dead| Rc::ptr_eq(live, dead)));
        self.inner.sessions.borrow_mut().push(Rc::new(move || {
            if let Some(shared) = weak.upgrade() {
                let wake = shared.state.borrow_mut().wake.take();
                if let Some(wake) = wake {
                    wake.wake();
                }
                true
            } else {
                false
            }
        }));
        (
            LatestSender {
                input: shared.clone(),
            },
            LatestResults { output: shared },
        )
    }
}
