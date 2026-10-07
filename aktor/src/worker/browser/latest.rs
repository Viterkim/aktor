use super::*;
use crate::latest::{SendLatest, Session};
use core::{
    future::poll_fn,
    ops::AsyncFnOnce,
    pin::Pin,
    task::{Context, Poll, Waker},
};

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
impl<O> futures_util::Stream for LatestResults<O> {
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
pub trait Service {
    fn operation(&self) -> &str;
    fn take(&self) -> Result<Option<Vec<u8>>, WireError>;
    fn answer(&self, output: Result<Vec<u8>, WireError>);
}

struct Shared<I, O> {
    inner: Rc<Inner>,
    operation: Operation,
    slot: RefCell<Slot<I, O>>,
    this: Weak<Self>,
    wake_callback: Rc<dyn Fn() -> bool>,
    encoder: fn(&I) -> Result<Vec<u8>, WorkerError>,
    decoder: fn(&[u8]) -> Result<O, WorkerError>,
}

struct Slot<I, O> {
    revision: u64,
    pending: Option<I>,
    output: Option<O>,
    running: Option<u64>,
    scheduled: bool,
    senders: usize,
    receiver: bool,
    failed: Option<WireError>,
    wake: Option<Waker>,
}

impl<I: 'static, O: 'static> Shared<I, O> {
    fn schedule(&self) {
        {
            let mut slot = self.slot.borrow_mut();

            if slot.scheduled
                || slot.running.is_some()
                || !slot.receiver
                || slot.pending.is_none()
                || slot.failed.is_some()
            {
                return;
            }

            slot.scheduled = true;
        }

        let id = self.inner.next.get();
        let Some(next) = id.checked_add(1) else {
            self.inner.fail(WorkerCause::Protocol);
            return;
        };

        self.inner.next.set(next);

        let Some(this) = self.this.upgrade() else {
            return;
        };

        self.inner.outstanding.borrow_mut().insert(
            id,
            Work {
                answer: None,
                input: None,
                service: Some(Box::new(ServiceCall(this))),
                _count: None,
                _bytes: None,
            },
        );

        self.inner.queue.borrow_mut().push_back(id);
        self.inner.pump();
    }

    fn wake(&self) {
        let wake = self.slot.borrow_mut().wake.take();

        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

struct ServiceCall<I, O>(Rc<Shared<I, O>>);
impl<I: 'static, O: 'static> Service for ServiceCall<I, O> {
    fn operation(&self) -> &str {
        self.0.operation.name
    }

    fn take(&self) -> Result<Option<Vec<u8>>, WireError> {
        let input = {
            let mut slot = self.0.slot.borrow_mut();

            slot.scheduled = false;

            if !slot.receiver {
                return Ok(None);
            }

            let Some(input) = slot.pending.take() else {
                return Ok(None);
            };

            slot.running = Some(slot.revision);
            input
        };

        (self.0.encoder)(&input)
            .map(Some)
            .map_err(|error| error.without_data())
    }

    fn answer(&self, output: Result<Vec<u8>, WireError>) {
        let output =
            output.and_then(|bytes| (self.0.decoder)(&bytes).map_err(|error| error.without_data()));
        let (discarded, pending) = {
            let mut slot = self.0.slot.borrow_mut();
            let revision = slot.running.take();

            match output {
                Ok(output) if slot.receiver && revision == Some(slot.revision) => {
                    (slot.output.replace(output), None)
                }
                Ok(output) => (Some(output), None),
                Err(error) => {
                    slot.failed = Some(error);
                    slot.scheduled = false;
                    (None, slot.pending.take())
                }
            }
        };

        drop(discarded);
        drop(pending);

        let failure = self.0.slot.borrow().failed.clone();

        if let Some(error) = failure {
            self.0.inner.fail(error.cause);
        }

        self.0.schedule();
        self.0.wake();
    }
}

impl<I: 'static, O: 'static> Input<I> for Shared<I, O> {
    fn send(&self, input: I) {
        let (pending, unread) = {
            let mut slot = self.slot.borrow_mut();

            if !slot.receiver || slot.failed.is_some() || self.inner.closed.get() {
                return;
            }

            slot.revision = slot.revision.wrapping_add(1);
            (slot.pending.replace(input), slot.output.take())
        };

        drop(pending);
        drop(unread);
        self.schedule();
    }

    fn clone_sender(&self) {
        self.slot.borrow_mut().senders += 1;
    }

    fn close_sender(&self) {
        let mut slot = self.slot.borrow_mut();

        slot.senders = slot.senders.saturating_sub(1);
        drop(slot);
        self.wake();
    }
}
impl<I, O> Output<O> for Shared<I, O> {
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<O>> {
        let replacement = cx.waker().clone();
        let mut slot = self.slot.borrow_mut();

        if let Some(output) = slot.output.take() {
            return Poll::Ready(Some(output));
        }

        if let Some(error) = slot.failed.clone() {
            drop(slot);
            return self.inner.lost(error);
        }

        let terminal = self
            .inner
            .finished
            .borrow()
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .cloned();

        if let Some(error) = terminal {
            drop(slot);
            return self.inner.lost(error);
        }

        if (slot.senders == 0 || self.inner.closed.get())
            && !slot.scheduled
            && slot.running.is_none()
            && slot.pending.is_none()
        {
            return Poll::Ready(None);
        }

        let previous = slot.wake.replace(replacement);

        drop(slot);
        drop(previous);
        Poll::Pending
    }

    fn close_receiver(&self) {
        let mut slot = self.slot.borrow_mut();

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
impl<S, I: 'static, O: 'static, Role, E, C> Session<S, I, O, Role, C> for &Worker<S, Role, E>
where
    C: Codec<I> + Codec<O>,
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F>(self, operation: Operation, _function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        self.inner.handles.set(self.inner.handles.get() + 1);

        let shared = Rc::new_cyclic(|this: &Weak<Shared<I, O>>| Shared {
            inner: self.inner.clone(),
            operation,
            encoder: <C as Codec<I>>::encode,
            decoder: <C as Codec<O>>::decode_output,
            this: this.clone(),
            wake_callback: Rc::new({
                let weak = this.clone();

                move || {
                    weak.upgrade().is_some_and(|shared| {
                        shared.wake();
                        true
                    })
                }
            }),
            slot: RefCell::new(Slot {
                revision: 0,
                pending: None,
                output: None,
                running: None,
                scheduled: false,
                senders: 1,
                receiver: true,
                failed: None,
                wake: None,
            }),
        });

        self.inner.register_session(&shared.wake_callback);
        (
            LatestSender {
                input: shared.clone(),
            },
            LatestResults { output: shared },
        )
    }
}

impl<I, O> Drop for Shared<I, O> {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;

        self.inner.handles.set(handles);

        if handles == 0 {
            self.inner.shutdown();
        }
    }
}

impl<S, I, O, Lease, Role, E, C> crate::latest::TypedSession<S, I, O, Lease, Role, C>
    for &Worker<S, Role, E>
where
    Self: crate::latest::Session<S, I, O, Role, C>,
{
    type Sender<Fut> = <Self as crate::latest::Session<S, I, O, Role, C>>::Sender;
    type Results = <Self as crate::latest::Session<S, I, O, Role, C>>::Results;

    fn session<F, Fut>(
        self,
        operation: Operation,
        function: F,
        _: fn(Lease, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: core::future::Future<Output = (Lease, O)>,
    {
        <Self as crate::latest::Session<S, I, O, Role, C>>::session(self, operation, function)
    }
}
