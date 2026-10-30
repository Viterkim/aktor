use super::*;
use crate::dispatch::{OwnedState, ReadState, Transport, WriteState};
use core::{
    pin::Pin,
    task::{Context, Poll, ready},
};

impl<S: 'static, Role: 'static, E> OptionalWorker<S, Role, E> {
    pub fn new(url: &str) -> Result<Self, WorkerError> {
        Self::with_options(url, Options::default())
    }

    pub fn with_options(url: &str, options: Options) -> Result<Self, WorkerError> {
        Ok(Self {
            worker: Ok(Worker::with_policy(url, options, true)?),
        })
    }

    pub fn with_config<Config: Serialize>(
        url: &str,
        options: Options,
        config: &Config,
    ) -> Result<Self, WorkerError> {
        let config = encode(config)?;
        let worker = Worker::with_policy(url, options, true)?;

        worker.inner.accepted.set(false);
        *worker.inner.initialize.borrow_mut() = Some(config);
        Ok(Self::from_worker(worker))
    }

    /// Connect the external join while constructing this handle.
    /// Refusal still runs the supplied join and retains this handle, without
    /// registering another shutdown dependency.
    pub fn before_shutdown(
        self,
        joining: impl Future<Output = Result<(), crate::AktorError>> + 'static,
    ) -> Result<Self, WorkerError>
    where
        E: 'static,
    {
        match self.worker {
            Ok(worker) => Ok(Self::from_worker(worker.before_shutdown(joining)?)),
            Err(unavailable) => {
                let error = unavailable.error.clone();
                let diagnostics = unavailable.completion.diagnostics.clone();
                drop(ShutdownDependency::new(Some(Box::pin(async move {
                    let result = joining.await;

                    if let Err(error) = &result {
                        diagnostics.borrow_mut().push(error.clone());
                    }

                    drop(unavailable);
                    result
                }))));

                Err(error)
            }
        }
    }

    pub async fn ready(&self) -> Result<(), WorkerError<E>>
    where
        E: DeserializeOwned,
    {
        match &self.worker {
            Ok(worker) => worker.ready().await,
            Err(unavailable) => Err(unavailable.error.clone().without_data()),
        }
    }

    pub fn new_handle(&self) -> Self {
        Self {
            worker: self
                .worker
                .as_ref()
                .map(Worker::new_handle)
                .map_err(|unavailable| Unavailable {
                    error: unavailable.error.clone(),
                    completion: unavailable.completion.new_observer(),
                }),
        }
    }

    pub fn shutdown(&self) -> Completion<E> {
        if let Ok(worker) = &self.worker {
            worker.shutdown();
        }

        self.completion()
    }

    pub fn completion(&self) -> Completion<E> {
        match &self.worker {
            Ok(worker) => worker.completion(),
            Err(unavailable) => unavailable.completion.new_observer(),
        }
    }

    pub fn terminate(&self) {
        if let Ok(worker) = &self.worker {
            worker.terminate();
        }
    }

    pub fn request<O: DeserializeOwned + 'static>(
        &self,
        operation: &str,
        input: Result<Vec<u8>, WorkerError>,
    ) -> OptionalRequest<'_, S, O, Role> {
        OptionalRequest {
            request: self
                .worker
                .as_ref()
                .map(|worker| worker.request(operation, input))
                .map_err(|unavailable| unavailable.error.clone()),
        }
    }

    pub(crate) fn unavailable(
        error: WorkerError,
        dependency: watch::Receiver<Option<Result<(), crate::AktorError>>>,
    ) -> Self {
        let completion = Completion::unavailable(error.clone(), dependency);

        Self {
            worker: Err(Unavailable { error, completion }),
        }
    }

    pub(crate) fn into_worker(self) -> Result<Worker<S, Role, E>, WorkerError> {
        self.worker.map_err(|unavailable| unavailable.error)
    }

    pub(crate) fn from_worker(worker: Worker<S, Role, E>) -> Self {
        Self { worker: Ok(worker) }
    }
}

impl<'a, S, O: 'static, Role> OptionalRequest<'a, S, O, Role> {
    pub async fn send(self) -> Result<OptionalReply<O>, WorkerError> {
        Ok(OptionalReply {
            reply: self.request?.admit().await?,
        })
    }
}
impl<S, O: 'static, Role> Future for OptionalRequest<'_, S, O, Role> {
    type Output = Result<O, WorkerError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let request = match &mut this.request {
            Ok(request) => request,
            Err(error) => return Poll::Ready(Err(error.clone())),
        };

        ready!(request.poll_submit(cx)).map_err(WireError::without_data)?;

        match &mut request.reply {
            Some(reply) => reply.poll_outcome(cx),
            None => Poll::Ready(Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Closed,
            ))),
        }
    }
}
impl<O> Future for OptionalReply<O> {
    type Output = Result<O, WorkerError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().reply.poll_outcome(cx)
    }
}
impl<'a, S, O: 'static, Role> crate::call::SendRequest for OptionalRequest<'a, S, O, Role> {
    type Reply = Result<OptionalReply<O>, WorkerError>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<'a, S, I: 'a, O: 'static, Role, E, C> Transport<S, I, O, Role, C>
    for &'a OptionalWorker<S, Role, E>
where
    C: Codec<I> + Codec<O>,
{
    type Request = OptionalRequest<'a, S, O, Role>;

    fn request(self, operation: Operation, input: I) -> Self::Request {
        OptionalRequest {
            request: self
                .worker
                .as_ref()
                .map(|worker| Transport::<S, I, O, Role, C>::request(worker, operation, input))
                .map_err(|unavailable| unavailable.error.clone()),
        }
    }
}
impl<S: 'static, Role, E> ReadState<S, Role> for &OptionalWorker<S, Role, E> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, Role, E> WriteState<S, Role> for &OptionalWorker<S, Role, E> {
    type Lease = OwnedState<S>;
}
