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
            data: PhantomData,
        }
    }

    pub fn terminate(&self) {
        self.inner.fail(WorkerCause::Closed);
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

    pub fn request<O: DeserializeOwned>(
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
impl<'a, S, I: 'a, O, Role, E, C> Transport<S, I, O, Role, C> for &'a Worker<S, Role, E>
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
