use super::*;
use crate::dispatch::{OwnedState, ReadState, Transport, WriteState};

pub mod inner;
mod reply;
mod request;

impl<S, Role, E> Worker<S, Role, E> {
    #[doc(hidden)]
    pub fn manage(&self, name: String, group: crate::group::KillSwitch) {
        *self.inner.group.borrow_mut() = Some((name, group));
    }

    pub async fn open(url: &str, options: Options) -> Result<Self, WorkerError<E>>
    where
        S: 'static,
        Role: 'static,
        E: DeserializeOwned,
    {
        let worker = Self::with_options(url, options).map_err(|error| error.without_data())?;

        worker.ready().await?;

        Ok(worker)
    }

    pub async fn ready(&self) -> Result<(), WorkerError<E>>
    where
        E: DeserializeOwned,
    {
        observe(self.inner.ready.subscribe())
            .await
            .map_err(completion::typed)
    }

    pub fn new_handle(&self) -> Self {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Self {
            inner: self.inner.clone(),
            state: PhantomData,
        }
    }

    pub fn shutdown(&self) -> Completion<E> {
        self.inner.shutdown();
        self.completion()
    }

    pub fn completion(&self) -> Completion<E> {
        Completion {
            result: self.inner.finished.subscribe(),
            diagnostics: self.inner.diagnostics.clone(),
            retired: self.inner.retired.clone(),
            data: PhantomData,
        }
    }

    pub fn terminate(&self) {
        self.inner.fail(WorkerCause::Closed);
        self.inner.continue_shutdown(true);
    }

    /// Connect an external owner's join before exposing the worker to calls.
    /// Once attached, normal and forced shutdown wait for it. Keep unconfirmed
    /// work owned inside this future until its physical owner has retired.
    /// Refusal still runs the supplied join and retains this handle, without
    /// registering another shutdown dependency.
    pub fn before_shutdown(
        self,
        joining: impl Future<Output = Result<(), crate::AktorError>> + 'static,
    ) -> Result<Self, WorkerError>
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
            drop(ShutdownDependency::new(Some(Box::pin(async move {
                let result = joining.await;

                if let Err(error) = &result {
                    diagnostics.borrow_mut().push(error.clone());
                }

                drop(self);
                result
            }))));

            return Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Closed,
            ));
        }

        *self.inner.shutdown_join.borrow_mut() = ShutdownJoin::Waiting(Box::pin(joining));

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
            <C as Codec<I>>::encode(&input).map_err(|error| error.without_data())
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
