use super::*;
use crate::dispatch::{OwnedState, ReadState, Transport, WriteState};

pub mod inner;
mod reply;
mod request;

impl<S, Role, E> Worker<S, Role, E> {
    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped. Returns `None` if the actor has no group.
    pub fn group_completion(&self) -> Option<crate::AktorGroupCompletion> {
        self.inner
            .group
            .borrow()
            .as_ref()
            .map(|(_, kill)| kill.completion())
    }

    /// Wrap the outermost future that needs to stop with this worker's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        let kill = self
            .inner
            .group
            .borrow()
            .as_ref()
            .map(|(_, kill)| kill.clone());

        match kill {
            Some(kill) => kill.run(work).await,
            None => Some(work.await),
        }
    }

    #[doc(hidden)]
    pub fn manage(&self, name: String, group: crate::group::AktorKillSwitch) {
        *self.inner.group.borrow_mut() = Some((name, group));
    }

    #[doc(hidden)]
    pub async fn open(url: &str, options: Options) -> Result<Self, (WorkerStartError<E>, String)>
    where
        S: 'static,
        Role: 'static,
        E: DeserializeOwned,
    {
        let worker = Self::with_policy(url, options, false).map_err(WorkerStartError::not_run)?;

        worker.ready().await?;

        Ok(worker)
    }

    /// Wait for startup. If the initializer reports a failure while it is still rolling back,
    /// `completion().wait_retired()` waits for the worker to stop and includes the final error if it arrives.
    pub async fn ready(&self) -> Result<(), (WorkerStartError<E>, String)>
    where
        E: DeserializeOwned,
    {
        observe(self.inner.ready.subscribe())
            .await
            .map_err(|failure| failure.report(completion::typed))
    }

    /// The same worker, for calling functions marked `#[aktor(role = R)]`.
    pub fn with_role<R>(self) -> Worker<S, R, E> {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Worker {
            inner: self.inner.clone(),
            state: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn shutdown(&self) -> Completion<E> {
        self.inner.shutdown();
        self.completion()
    }

    /// Keep this to observe retirement or wait for this worker's `end`.
    pub fn completion(&self) -> Completion<E> {
        Completion {
            result: self.inner.finished.subscribe(),
            diagnostics: self.inner.diagnostics.clone(),
            retirement: self.inner.retirement.subscribe(),
            end: self.inner.end.subscribe(),
            data: PhantomData,
        }
    }

    /// Request termination after any accepted external join finishes.
    /// The worker can stay alive while it retains unfinished work.
    pub fn terminate(&self) {
        self.inner.fail(WorkerCause::Closed);
        self.inner.continue_shutdown(true);
    }

    pub(crate) fn give_up(&self, stuck: crate::AktorStuck) -> bool {
        // A pending startup needs an answer even when its initializer is left running.
        if stuck == crate::AktorStuck::Kill || self.inner.ready.borrow().is_none() {
            self.terminate();
        }

        *self.inner.retirement.borrow() == AktorRetireStatus::Terminated
    }

    pub(crate) async fn group_outcome(
        &self,
        stuck: crate::AktorStuck,
    ) -> (AktorRetireStatus, Vec<crate::AktorError>) {
        let completion = self.completion();
        let status = match stuck {
            crate::AktorStuck::Kill => completion.await_retirement().await,
            crate::AktorStuck::Leave => completion.retirement_status().await,
        };

        (status, self.inner.diagnostics.borrow().clone())
    }

    /// Attach an external join when you construct the worker.
    /// Normal and forced shutdown wait for an accepted join. Keep its resources here until
    /// the external work stops. Return an error only once the work has stopped.
    ///
    /// A refused join runs independently and keeps this handle alive until it finishes.
    pub fn before_shutdown<Output: crate::AktorCleanupOutput<Mode>, Mode>(
        self,
        joining: impl Future<Output = Output> + 'static,
    ) -> Result<Self, (WorkerError, String)>
    where
        S: 'static,
        Role: 'static,
        E: 'static,
    {
        self.attach_shutdown(super::dependency::reported(joining))
            .map_err(Into::into)
    }

    pub(crate) fn attach_shutdown(
        self,
        joining: LocalFuture<'static, Result<(), crate::AktorError>>,
    ) -> Result<Self, WorkerFailure>
    where
        S: 'static,
        Role: 'static,
        E: 'static,
    {
        let occupied = match &*self.inner.shutdown_join.borrow() {
            ShutdownJoin::Ready => false,
            ShutdownJoin::Waiting(_) | ShutdownJoin::Joining { .. } => true,
        };

        let refused = occupied
            || self.inner.ready.borrow().is_some()
            || self.inner.next.get() != 0
            || self.inner.closed.get()
            || self.inner.handles.get() != 1;

        if refused {
            let diagnostics = self.inner.diagnostics.clone();
            wasm_bindgen_futures::spawn_local(async move {
                if let Err(error) = joining.await {
                    diagnostics.borrow_mut().push(error);
                }

                drop(self);
            });

            return Err(WorkerFailure::new(
                CallError::NotAdmitted,
                WorkerCause::Closed,
            ));
        }

        *self.inner.shutdown_join.borrow_mut() = ShutdownJoin::Waiting(joining);

        Ok(self)
    }

    pub fn executing(&self) -> bool {
        self.inner.executing.get()
    }

    pub fn outstanding(&self) -> (usize, usize) {
        (
            self.inner.outstanding.borrow().len(),
            self.inner.options.max_outstanding_bytes - self.inner.bytes.available_permits(),
        )
    }

    pub fn request<O: DeserializeOwned + 'static>(
        &self,
        operation: &str,
        input: Result<Vec<u8>, WorkerError>,
    ) -> WorkerRequest<'_, S, O, Role> {
        WorkerRequest::new(self, operation, input)
    }
}
impl<S, Role, E> Clone for Worker<S, Role, E> {
    fn clone(&self) -> Self {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Self {
            inner: self.inner.clone(),
            state: PhantomData,
        }
    }
}
impl<S, Role, E> Drop for Worker<S, Role, E> {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;

        self.inner.handles.set(handles);

        if handles == 0 {
            self.inner.shutdown();
        }
    }
}
impl<'a, S, I: 'a, O: 'static, Role, E, C> Transport<S, I, O, Role, C> for &'a Worker<S, Role, E>
where
    C: Codec<I> + Codec<O>,
{
    type Request = WorkerRequest<'a, S, O, Role>;

    fn request(self, operation: Operation, input: I) -> Self::Request {
        let mut request = WorkerRequest::with_decoder(
            self,
            operation.name,
            Ok(Vec::new()),
            <C as Codec<O>>::decode_output,
        );

        request.input = None;
        request.encoder = Some(Box::new(move || {
            <C as Codec<I>>::encode(&input).map_err(WireError::from)
        }));
        request
    }
}

pub fn fatal(error: WireError) -> ! {
    std::panic::resume_unwind(Box::new(error))
}

impl<S: 'static, Role, E> ReadState<S, Role> for &Worker<S, Role, E> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, Role, E> WriteState<S, Role> for &Worker<S, Role, E> {
    type Lease = OwnedState<S>;
}
