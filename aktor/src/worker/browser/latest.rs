use super::*;
use crate::latest::{SendLatest, Session, WorkerSession};
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
    pub async fn next(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        poll_fn(|cx| self.output.poll(cx)).await
    }
}
impl<O> futures_util::Stream for LatestResults<O> {
    type Item = Result<O, crate::AktorDeliveryError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<O, crate::AktorDeliveryError>>> {
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
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<Result<O, crate::AktorDeliveryError>>>;
    fn close_receiver(&self);
}
#[derive(Er)]
pub(super) struct EncodeLatestErr {
    pub(super) cause: WorkerCause,
}
pub(super) trait Service {
    fn operation(&self) -> &str;
    fn take(&self) -> ErResult<Option<Vec<u8>>, EncodeLatestErr>;
    fn answer(
        &self,
        output: Result<Vec<u8>, WireFailure>,
        record: &dyn Fn(WireFailure) -> WireFailure,
    ) -> Option<WireFailure>;
}

struct Shared<I, O> {
    inner: Rc<Inner>,
    operation: Operation,
    slot: RefCell<Slot<I, O>>,
    this: Weak<Self>,
    wake_callback: Rc<dyn Fn() -> bool>,
    encoder: Encoder<I>,
    decoder: Decoder<O>,
}

struct Slot<I, O> {
    revision: u64,
    pending: Option<I>,
    output: Option<O>,
    running: Option<u64>,
    scheduled: Option<u64>,
    senders: usize,
    receiver: bool,
    failed: Option<WireFailure>,
    wake: Option<Waker>,
}

impl<I: 'static, O: 'static> Shared<I, O> {
    fn schedule(&self) {
        {
            let slot = self.slot.borrow();

            if slot.scheduled.is_some()
                || slot.running.is_some()
                || !slot.receiver
                || slot.pending.is_none()
                || slot.failed.is_some()
            {
                return;
            }
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

        self.slot.borrow_mut().scheduled = Some(id);
        self.inner.outstanding.borrow_mut().insert(
            id,
            Work {
                operation: self.operation,
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

    fn take(&self) -> ErResult<Option<Vec<u8>>, EncodeLatestErr> {
        let input = {
            let mut slot = self.0.slot.borrow_mut();

            slot.scheduled = None;

            if !slot.receiver {
                return Ok(None);
            }

            let Some(input) = slot.pending.take() else {
                return Ok(None);
            };

            slot.running = Some(slot.revision);
            input
        };

        let bytes = crate::worker::wire::codec((self.0.encoder)(&input), || EncodeLatestErr {
            cause: WorkerCause::Codec,
        })?;
        Ok(Some(bytes))
    }

    fn answer(
        &self,
        output: Result<Vec<u8>, WireFailure>,
        record: &dyn Fn(WireFailure) -> WireFailure,
    ) -> Option<WireFailure> {
        let output = output.and_then(|bytes| {
            {
                let slot = self.0.slot.borrow();
                if !slot.receiver || slot.running != Some(slot.revision) {
                    return Ok(None);
                }
            }

            Ok(Some((self.0.decoder)(&bytes)?))
        });
        let output = match output {
            Ok(value) => Ok(value),
            Err(error) => Err(record(error)),
        };
        let (discarded, pending) = {
            let mut slot = self.0.slot.borrow_mut();
            let revision = slot.running.take();

            match output {
                Ok(Some(output)) if slot.receiver && revision == Some(slot.revision) => {
                    (slot.output.replace(output), None)
                }
                Ok(output) => (output, None),
                Err(error) => {
                    slot.failed = Some(error);
                    slot.scheduled = None;
                    (None, slot.pending.take())
                }
            }
        };

        drop(discarded);
        drop(pending);

        let failure = self.0.slot.borrow().failed.clone();

        self.0.schedule();
        self.0.wake();
        failure
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
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<Result<O, crate::AktorDeliveryError>>> {
        let replacement = cx.waker().clone();
        let mut slot = self.slot.borrow_mut();

        if !slot.receiver {
            return Poll::Ready(None);
        }

        if slot.failed.is_none()
            && let Some(output) = slot.output.take()
        {
            return Poll::Ready(Some(Ok(output)));
        }

        if let Some(error) = slot.failed.take().or_else(|| {
            self.inner
                .recorded_failure(AktorDeliveryOutcome::OutcomeUnknown)
        }) {
            slot.receiver = false;
            drop(slot);
            deliver(error.error.failure_id, &self.inner.group);
            return Poll::Ready(Some(Err(error.into())));
        }

        if (slot.senders == 0 || self.inner.finished.borrow().is_some())
            && slot.scheduled.is_none()
            && slot.running.is_none()
            && slot.pending.is_none()
        {
            slot.receiver = false;
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
        let scheduled = slot.scheduled.take();

        drop(slot);
        if let Some(id) = scheduled {
            self.inner.cancel_queued(id);
        }
        drop(pending);
        drop(output);
        drop(wake);
    }
}
impl<S, I: 'static, O: 'static, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    Session<S, I, O, Role, C> for &Worker<W>
where
    C: Codec<I> + Codec<O>,
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F>(self, operation: Operation, _function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        <Self as WorkerSession<S, I, O, Role, C>>::open_session(self, operation)
    }
}

impl<S, I: 'static, O: 'static, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    WorkerSession<S, I, O, Role, C> for &Worker<W>
where
    C: Codec<I> + Codec<O>,
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn open_session(self, operation: Operation) -> (Self::Sender, Self::Results) {
        self.inner.handles.set(self.inner.handles.get() + 1);

        let shared = Rc::new_cyclic(|this: &Weak<Shared<I, O>>| Shared {
            inner: self.inner.clone(),
            operation,
            encoder: <C as Codec<I>>::encode,
            decoder: decode_output::<C, O>,
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
                scheduled: None,
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

impl<S, I, O, Lease, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    crate::latest::TypedSession<S, I, O, Lease, Role, C> for &Worker<W>
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

impl<S, I: 'static, O: 'static, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    Session<S, I, O, Role, C> for &super::optional::OptionalWorker<W>
where
    C: Codec<I> + Codec<O>,
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn session<F>(self, operation: Operation, _function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        <Self as WorkerSession<S, I, O, Role, C>>::open_session(self, operation)
    }
}

impl<S, I, O, Lease, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    crate::latest::TypedSession<S, I, O, Lease, Role, C> for &super::optional::OptionalWorker<W>
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

impl<S, I: 'static, O: 'static, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    WorkerSession<S, I, O, Role, C> for &super::optional::OptionalWorker<W>
where
    C: Codec<I> + Codec<O>,
{
    type Sender = LatestSender<I>;
    type Results = LatestResults<O>;

    fn open_session(self, operation: Operation) -> (Self::Sender, Self::Results) {
        match self.session_worker() {
            Ok(worker) => {
                <&Worker<W> as WorkerSession<S, I, O, Role, C>>::open_session(worker, operation)
            }
            Err((group, failure)) => (
                LatestSender {
                    input: Rc::new(ClosedInput),
                },
                LatestResults {
                    output: Rc::new(UnavailableOutput {
                        group,
                        failure: RefCell::new(Some(
                            failure.with_outcome(AktorDeliveryOutcome::NotAdmitted),
                        )),
                    }),
                },
            ),
        }
    }
}

struct ClosedInput;

impl<I> Input<I> for ClosedInput {
    fn send(&self, _: I) {}
    fn clone_sender(&self) {}
    fn close_sender(&self) {}
}

struct UnavailableOutput {
    group: crate::AktorKillSwitch,
    failure: RefCell<Option<WireFailure>>,
}

impl<O> Output<O> for UnavailableOutput {
    fn poll(&self, _: &mut Context<'_>) -> Poll<Option<Result<O, crate::AktorDeliveryError>>> {
        Poll::Ready(self.failure.borrow_mut().take().map(|failure| {
            deliver(failure.error.failure_id, &self.group);
            Err(failure.into())
        }))
    }

    fn close_receiver(&self) {}
}
