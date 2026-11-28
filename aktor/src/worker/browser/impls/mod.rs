pub mod inner;

mod reply;
mod request;

pub use request::{WorkerRequestCast, WorkerRequestSend};

use super::*;
use crate::dispatch::{OwnedState, ReadState, Transport, WriteState};

impl<W: AktorWorkerContract> Worker<W> {
    /// The registered launch attempt this handle belongs to.
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        self.inner.group.registered_id()
    }

    pub fn group_completion(&self) -> crate::AktorGroupCompletion {
        self.inner.group.completion()
    }

    /// Drop the future when this worker's group starts closing.
    pub fn run<F: IntoFuture>(&self, work: F) -> crate::AktorRun<F::IntoFuture> {
        self.inner.group.run(work.into_future())
    }

    /// The payload returned by the worker's `start`, available after startup succeeds.
    /// Wait for startup. If the initializer reports a failure while it is still rolling back,
    /// `completion().wait_retired()` waits for the worker to stop and includes the final error if it arrives.
    pub async fn ready(&self) -> Result<&W::Ready, (&WorkerStartError<W::Error>, String)> {
        if self.receipt.get().is_none()
            && let Err(error) = self.observe_ready().await
        {
            let _ = self.receipt.set(Err(error));
        }

        match self
            .receipt
            .get()
            .expect("worker published its startup receipt")
        {
            Ok(payload) => Ok(payload),
            Err((error, text)) => {
                deliver(error.failure().failure_id, &self.inner.group);
                Err((error, text.clone()))
            }
        }
    }

    pub(crate) async fn observe_ready(&self) -> Result<(), (WorkerStartError<W::Error>, String)> {
        match observe(self.inner.ready.subscribe()).await {
            Ok(()) => Ok(()),
            Err(failure) => Err(failure.report(completion::typed)),
        }
    }

    #[doc(hidden)]
    pub fn shutdown(&self) -> Completion<W::Error> {
        self.inner.shutdown();
        self.completion()
    }

    /// Keep this to observe retirement or wait for this worker's `end`.
    pub fn completion(&self) -> Completion<W::Error> {
        Completion {
            result: self.inner.finished.subscribe(),
            group: self.inner.group.clone(),
            retirement: self.inner.retirement.subscribe(),
            end: self.inner.end.subscribe(),
            data: PhantomData,
        }
    }

    /// Request termination after any accepted external join finishes.
    /// The worker can stay alive while it retains unfinished work.
    pub fn terminate(&self) {
        if self.inner.retirement.borrow().is_some() || self.inner.terminate_requested.replace(true)
        {
            return;
        }
        self.inner.fail(WorkerCause::Closed);
        self.inner.continue_shutdown(true);
    }

    pub(crate) fn control(&self) -> WorkerControl<W::Error> {
        WorkerControl {
            inner: self.inner.clone(),
            data: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn executing(&self) -> bool {
        self.inner.executing.get()
    }

    #[doc(hidden)]
    pub fn outstanding(&self) -> (usize, usize) {
        (
            self.inner.outstanding.borrow().len(),
            self.inner.options.max_outstanding_bytes.get() - self.inner.bytes.available_permits(),
        )
    }
}
impl<W: AktorWorkerContract> Clone for Worker<W> {
    fn clone(&self) -> Self {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Self {
            state: PhantomData,
            receipt: self.receipt.clone(),
            inner: self.inner.clone(),
        }
    }
}
impl<W: AktorWorkerContract, S> Drop for WorkerHandle<W, S> {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;

        self.inner.handles.set(handles);

        if handles == 0 {
            self.inner.shutdown();
        }
    }
}
impl<'a, S, I: 'a, O: 'static, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    Transport<S, I, O, Role, C> for &'a Worker<W>
where
    C: Codec<I> + Codec<O>,
{
    type Request = WorkerRequest<'a, S, O, Role>;

    fn request(self, operation: Operation, input: I) -> Self::Request {
        WorkerRequest {
            inner: &self.inner,
            state: PhantomData,
            operation,
            input: None,
            encoder: Some(Box::new(move || {
                Ok(crate::worker::wire::codec(
                    <C as Codec<I>>::encode(&input),
                    || WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Codec),
                )?)
            })),
            decoder: decode_output::<C, O>,
            admission: None,
            reply: None,
        }
    }
}

/// Non-owning supervision handle for a browser worker. Cloning or dropping it does not change
/// the worker's user handle count, so dropping the last user `Worker` stops the worker.
pub(crate) struct WorkerControl<E> {
    inner: Rc<Inner>,
    data: PhantomData<fn() -> E>,
}
impl<E> Clone for WorkerControl<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            data: PhantomData,
        }
    }
}
impl<E> WorkerControl<E> {
    pub(crate) fn shutdown(&self) {
        self.inner.shutdown();
    }

    pub(crate) fn give_up(&self, stuck: crate::AktorStuck) -> bool {
        if stuck == crate::AktorStuck::Kill || self.inner.ready.borrow().is_none() {
            let lost_call = self.inner.active.get().is_some();
            self.inner.close_with_error(
                WireFailure::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Closed),
                lost_call,
            );
            self.inner.continue_shutdown(true);
        }

        *self.inner.retirement.borrow() == Some(AktorRetirement::Terminated)
    }

    pub(crate) fn init(&self) -> crate::AktorInitEvidence {
        match self.inner.ready.borrow().as_ref() {
            Some(Ok(())) => crate::AktorInitEvidence::Ready,
            Some(Err(failure)) => failure.evidence,
            _ => crate::AktorInitEvidence::Unknown,
        }
    }
}

#[cfg(feature = "wasm_browser_worker_server")]
pub fn fatal(error: WireFailure) -> ! {
    wasm_bindgen::throw_str(&error.report)
}

impl<S: 'static, W: AktorWorkerContract<State = S>> ReadState<S, W::Role> for &WorkerHandle<W, S> {
    type Lease = OwnedState<W::State>;
}
impl<S: 'static, W: AktorWorkerContract<State = S>> WriteState<S, W::Role> for &WorkerHandle<W, S> {
    type Lease = OwnedState<W::State>;
}
