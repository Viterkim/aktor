mod impls;
pub use impls::OptionalRequestSend;

use super::*;

/// A worker whose failure allows its group to keep running.
pub type OptionalWorker<C> = OptionalHandle<C, <C as AktorWorkerContract>::State>;

#[doc(hidden)]
pub struct OptionalHandle<W: AktorWorkerContract, S> {
    worker: Result<WorkerHandle<W, S>, Unavailable<W::Error>>,
}

#[must_use = "await the request or send it"]
pub struct OptionalRequest<'a, S, O, Role = ()> {
    request: Result<WorkerRequest<'a, S, O, Role>, (&'a crate::AktorKillSwitch, WireFailure)>,
}

struct Unavailable<E> {
    startup: Rc<(WorkerStartError<E>, String)>,
    failure: ReadyFailure,
    completion: Completion<E>,
    /// Keep the group so `run` stops when the group starts closing, even if the worker failed.
    group: crate::group::AktorKillSwitch,
    name: String,
}
