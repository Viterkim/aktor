use super::*;
use crate::dispatch::{OwnedState, ReadState, Transport, WriteState};
use core::{
    pin::Pin,
    task::{Context, Poll, ready},
};

impl<S: 'static, Role: 'static, E> OptionalWorker<S, Role, E> {
    /// Wrap the outermost future that needs to stop with this worker's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        match &self.worker {
            Ok(worker) => worker.run(work).await,
            Err(unavailable) => {
                let group = unavailable.group.borrow().clone();

                match group {
                    Some(kill) => kill.run(work).await,
                    None => Some(work.await),
                }
            }
        }
    }

    /// The same worker, for calling functions marked `#[aktor(role = R)]`.
    pub fn with_role<R: 'static>(self) -> OptionalWorker<S, R, E> {
        OptionalWorker {
            worker: match self.worker {
                Ok(worker) => Ok(worker.with_role()),
                Err(unavailable) => Err(unavailable),
            },
        }
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped. Returns `None` if the actor has no group.
    pub fn group_completion(&self) -> Option<crate::AktorGroupCompletion> {
        match &self.worker {
            Ok(worker) => worker.group_completion(),
            Err(unavailable) => unavailable
                .group
                .borrow()
                .as_ref()
                .map(|kill| kill.completion()),
        }
    }

    pub(crate) fn manage_unavailable(&self, kill: crate::group::AktorKillSwitch) {
        if let Err(unavailable) = &self.worker {
            *unavailable.group.borrow_mut() = Some(kill);
        }
    }

    #[doc(hidden)]
    pub fn new(url: &str) -> Result<Self, (WorkerError, String)> {
        Self::with_options(url, Options::default())
    }

    #[doc(hidden)]
    pub fn with_options(url: &str, options: Options) -> Result<Self, (WorkerError, String)> {
        Worker::with_policy(url, options, true)
            .map(Self::from_worker)
            .map_err(Into::into)
    }

    #[doc(hidden)]
    pub fn with_config<Config: Serialize>(
        url: &str,
        options: Options,
        config: &Config,
    ) -> Result<Self, (WorkerError, String)> {
        Worker::configured(url, options, config, true)
            .map(Self::from_worker)
            .map_err(Into::into)
    }

    /// Attach an external join during construction.
    /// Normal and forced shutdown wait for accepted joins.
    /// A refused join runs independently and keeps this handle alive until it finishes.
    pub fn before_shutdown<Output: crate::AktorCleanupOutput<Mode>, Mode>(
        self,
        joining: impl Future<Output = Output> + 'static,
    ) -> Result<Self, (WorkerError, String)>
    where
        E: 'static,
    {
        match self.worker {
            Ok(worker) => Ok(Self::from_worker(worker.before_shutdown(joining)?)),
            Err(unavailable) => {
                let joining = super::dependency::reported(joining);
                let error = unavailable.failure.error.clone();
                let diagnostics = unavailable.completion.diagnostics.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    if let Err(error) = joining.await {
                        diagnostics.borrow_mut().push(error);
                    }

                    drop(unavailable);
                });

                Err(error.into())
            }
        }
    }

    /// Wait for startup with the same rollback reporting as [`Worker::ready`].
    pub async fn ready(&self) -> Result<(), (WorkerStartError<E>, String)>
    where
        E: DeserializeOwned,
    {
        match &self.worker {
            Ok(worker) => worker.ready().await,
            Err(unavailable) => Err(unavailable
                .failure
                .clone()
                .report(WorkerFailure::report_without_data)),
        }
    }

    fn new_handle(&self) -> Self {
        Self {
            worker: self
                .worker
                .as_ref()
                .map(Worker::clone)
                .map_err(|unavailable| Unavailable {
                    failure: unavailable.failure.clone(),
                    completion: unavailable.completion.clone(),
                    group: unavailable.group.clone(),
                }),
        }
    }

    #[doc(hidden)]
    pub fn shutdown(&self) -> Completion<E> {
        if let Ok(worker) = &self.worker {
            worker.shutdown();
        }

        self.completion()
    }

    /// Keep this to observe retirement or wait for this worker's `end`.
    pub fn completion(&self) -> Completion<E> {
        match &self.worker {
            Ok(worker) => worker.completion(),
            Err(unavailable) => unavailable.completion.clone(),
        }
    }

    /// Request termination after any accepted external join finishes.
    /// The worker can stay alive while it retains unfinished work.
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
                .map_err(|unavailable| unavailable.failure.error.clone()),
        }
    }

    pub(crate) fn unavailable(
        failure: ReadyFailure<()>,
        dependency: watch::Receiver<Option<Result<(), crate::AktorError>>>,
    ) -> Self {
        let completion = Completion::unavailable(failure.error.clone(), dependency);

        Self {
            worker: Err(Unavailable {
                failure,
                completion,
                group: alloc::rc::Rc::new(core::cell::RefCell::new(None)),
            }),
        }
    }

    pub(crate) fn from_worker(worker: Worker<S, Role, E>) -> Self {
        Self { worker: Ok(worker) }
    }
}

impl<'a, S, O: 'static, Role> OptionalRequest<'a, S, O, Role> {
    pub async fn send(self) -> Result<OptionalReply<O>, (WorkerError, String)> {
        Ok(OptionalReply {
            reply: self
                .request
                .map_err(<(WorkerError, String)>::from)?
                .admit()
                .await?,
        })
    }
}
impl<S, O: 'static, Role> Future for OptionalRequest<'_, S, O, Role> {
    type Output = Result<O, (WorkerError, String)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let request = match &mut this.request {
            Ok(request) => request,
            Err(error) => return Poll::Ready(Err(error.clone().into())),
        };

        ready!(request.poll_submit(cx)).map_err(WireError::report_without_data)?;

        match &mut request.reply {
            Some(reply) => reply.poll_outcome(cx),
            None => Poll::Ready(Err(WorkerFailure::new(
                CallError::NotAdmitted,
                WorkerCause::Closed,
            )
            .into())),
        }
    }
}
impl<O> Future for OptionalReply<O> {
    type Output = Result<O, (WorkerError, String)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().reply.poll_outcome(cx)
    }
}
impl<'a, S, O: 'static, Role> crate::call::SendRequest for OptionalRequest<'a, S, O, Role> {
    type Reply = Result<OptionalReply<O>, (WorkerError, String)>;

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
                .map_err(|unavailable| unavailable.failure.error.clone()),
        }
    }
}
impl<S: 'static, Role: 'static, E> Clone for OptionalWorker<S, Role, E> {
    fn clone(&self) -> Self {
        self.new_handle()
    }
}
impl<S: 'static, Role, E> ReadState<S, Role> for &OptionalWorker<S, Role, E> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, Role, E> WriteState<S, Role> for &OptionalWorker<S, Role, E> {
    type Lease = OwnedState<S>;
}
