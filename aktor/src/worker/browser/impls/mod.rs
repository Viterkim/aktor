use super::*;
use crate::target::Transport;

mod inner;
mod reply;
mod request;

impl<S, Role> Worker<S, Role> {
    pub async fn open(url: &str, options: Options) -> Result<Self, WorkerError> {
        let worker = Self::with_options(url, options)?;
        worker.ready().await?;

        Ok(worker)
    }

    pub async fn ready(&self) -> Result<(), WorkerError> {
        observe(self.inner.ready.subscribe()).await
    }

    pub fn new_handle(&self) -> Self {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Self {
            inner: self.inner.clone(),
            state: PhantomData,
        }
    }

    pub fn shutdown(&self) -> Completion {
        self.inner.shutdown();
        self.completion()
    }

    pub fn completion(&self) -> Completion {
        Completion {
            result: self.inner.finished.subscribe(),
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
        input: Result<String, WorkerError>,
    ) -> WorkerRequest<'_, S, O, Role> {
        WorkerRequest::new(self, operation, input)
    }
}
impl<S, Role> Drop for Worker<S, Role> {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;
        self.inner.handles.set(handles);

        if handles == 0 {
            self.inner.shutdown();
        }
    }
}
impl<'a, S, I: Serialize, O: DeserializeOwned, Role> Transport<S, I, O, Role>
    for &'a Worker<S, Role>
{
    type Request = WorkerRequest<'a, S, O, Role>;

    fn request(self, operation: Operation, input: I) -> Self::Request {
        self.request(operation.name, encode(&input))
    }
}

pub fn fatal(error: WorkerError) -> ! {
    std::panic::resume_unwind(Box::new(error))
}
