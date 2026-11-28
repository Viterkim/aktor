use super::*;
use crate::dispatch::{OwnedState, ReadState, Transport, WriteState};
use core::{
    pin::Pin,
    task::{Context, Poll, ready},
};

impl<W: AktorWorkerContract> OptionalWorker<W> {
    pub(in crate::worker::browser) fn session_worker(
        &self,
    ) -> Result<&Worker<W>, (crate::AktorKillSwitch, WireFailure)> {
        match &self.worker {
            Ok(worker) => Ok(worker),
            Err(unavailable) => Err((unavailable.group.clone(), unavailable.failure.error.clone())),
        }
    }
}

impl<W: AktorWorkerContract> OptionalWorker<W> {
    /// Wrap the outermost future that needs to stop with this worker's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub fn run<F: IntoFuture>(&self, work: F) -> crate::AktorRun<F::IntoFuture> {
        let work = work.into_future();

        match &self.worker {
            Ok(worker) => worker.run(work),
            Err(unavailable) => unavailable.group.run(work),
        }
    }

    /// The registered launch attempt this handle belongs to.
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        match &self.worker {
            Ok(worker) => worker.actor_id(),
            Err(unavailable) => unavailable.group.registered_id(),
        }
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped.
    pub fn group_completion(&self) -> crate::AktorGroupCompletion {
        match &self.worker {
            Ok(worker) => worker.group_completion(),
            Err(unavailable) => unavailable.group.completion(),
        }
    }

    /// Wait for startup with the same rollback reporting as [`Worker::ready`].
    pub async fn ready(&self) -> Result<&W::Ready, (&WorkerStartError<W::Error>, String)> {
        match &self.worker {
            Ok(worker) => worker.ready().await,
            Err(unavailable) => {
                let (error, text) = &*unavailable.startup;
                deliver(error.failure().failure_id, &unavailable.group);
                Err((error, text.clone()))
            }
        }
    }

    #[doc(hidden)]
    pub fn shutdown(&self) -> Completion<W::Error> {
        if let Ok(worker) = &self.worker {
            worker.shutdown();
        }

        self.completion()
    }

    /// Keep this to observe retirement or wait for this worker's `end`.
    pub fn completion(&self) -> Completion<W::Error> {
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

    pub(crate) fn unavailable(
        failure: ReadyFailure,
        name: String,
        kill: crate::AktorKillSwitch,
        dependency: Option<JoinReceipt>,
    ) -> Self {
        let completion = Completion::unavailable(
            failure.error.clone(),
            name.clone(),
            kill.clone(),
            dependency,
        );

        Self {
            worker: Err(Unavailable {
                startup: Rc::new(failure.clone().report(WireFailure::without_data)),
                failure,
                completion,
                group: kill,
                name,
            }),
        }
    }

    pub(crate) fn from_worker(worker: Worker<W>) -> Self {
        Self { worker: Ok(worker) }
    }
}

impl<'a, S, O: 'static, Role> OptionalRequest<'a, S, O, Role> {
    fn available(
        &mut self,
    ) -> Result<&mut WorkerRequest<'a, S, O, Role>, crate::AktorDeliveryError> {
        match &mut self.request {
            Ok(request) => Ok(request),
            Err((group, error)) => {
                deliver(error.error.failure_id, group);
                Err(error.with_outcome(AktorDeliveryOutcome::NotAdmitted).into())
            }
        }
    }

    pub fn send(self) -> OptionalRequestSend<'a, S, O, Role> {
        OptionalRequestSend(self)
    }
}

impl<'a, S: 'a, Role: 'a> OptionalRequest<'a, S, (), Role> {
    pub async fn cast(self) -> Result<(), crate::AktorDeliveryError> {
        self.send().await.map(drop)
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct OptionalRequestSend<'a, S, O, Role = ()>(OptionalRequest<'a, S, O, Role>);
impl<S, O, Role> Unpin for OptionalRequestSend<'_, S, O, Role> {}
impl<S, O: 'static, Role> Future for OptionalRequestSend<'_, S, O, Role> {
    type Output = Result<WorkerReply<O>, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let request = self.get_mut().0.available()?;

        if let Err(error) = ready!(request.poll_checked(cx)) {
            return Poll::Ready(Err(error.into()));
        }

        match request.reply.take() {
            Some(reply) => Poll::Ready(Ok(reply)),
            None => Poll::Ready(Err(WireFailure::new(
                AktorDeliveryOutcome::NotAdmitted,
                WorkerCause::Closed,
            )
            .into())),
        }
    }
}

impl<S, O: 'static, Role> Future for OptionalRequest<'_, S, O, Role> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(self.get_mut().available()?).poll(cx)
    }
}
impl<'a, S, O: 'static, Role> crate::call::SendRequest for OptionalRequest<'a, S, O, Role> {
    type Reply = WorkerReply<O>;
    type Send = OptionalRequestSend<'a, S, O, Role>;

    fn send(self) -> Self::Send {
        self.send()
    }
}
impl<'a, S, I: 'a, O: 'static, Role, W: AktorWorkerContract<State = S, Role = Role>, C>
    Transport<S, I, O, Role, C> for &'a OptionalWorker<W>
where
    C: Codec<I> + Codec<O>,
{
    type Request = OptionalRequest<'a, S, O, Role>;

    fn request(self, operation: Operation, input: I) -> Self::Request {
        OptionalRequest {
            request: match &self.worker {
                Ok(worker) => Ok(Transport::<S, I, O, Role, C>::request(
                    worker, operation, input,
                )),
                Err(unavailable) => Err((&unavailable.group, unavailable.failure.error.clone())),
            },
        }
    }
}
impl<W: AktorWorkerContract> Clone for OptionalWorker<W> {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
        }
    }
}
impl<S: 'static, W: AktorWorkerContract<State = S>> ReadState<S, W::Role>
    for &OptionalHandle<W, S>
{
    type Lease = OwnedState<W::State>;
}
impl<S: 'static, W: AktorWorkerContract<State = S>> WriteState<S, W::Role>
    for &OptionalHandle<W, S>
{
    type Lease = OwnedState<W::State>;
}

impl<E> Clone for Unavailable<E> {
    fn clone(&self) -> Self {
        Self {
            failure: self.failure.clone(),
            startup: self.startup.clone(),
            completion: self.completion.clone(),
            group: self.group.clone(),
            name: self.name.clone(),
        }
    }
}

impl<'a, S: 'a, Role: 'a> crate::call::CastRequest for OptionalRequest<'a, S, (), Role> {
    type Output = Result<(), crate::AktorDeliveryError>;
    type Cast = crate::message::LocalFuture<'a, Self::Output>;

    fn cast(self) -> Self::Cast {
        Box::pin(self.cast())
    }
}
