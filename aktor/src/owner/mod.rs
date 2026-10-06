use crate::listener::{Actor, CleanupErrors, DedicatedJoinError, Handle};
use std::sync::Arc;
use tokio::sync::watch;

mod dispatch;
mod impls;
mod opaque_dispatch;

/// The resource on its own thread, with a handle for calling your functions.
pub struct Aktor<S, E, C, Role = ()> {
    pub handle: Handle<S, Role>,
    pub actor: Actor<S, E, C>,
    pub completion: OwnerCompletion<C>,
}

/// Wait for the result without keeping the actor alive.
pub struct OwnerCompletion<C> {
    result: watch::Receiver<Option<Result<(), Arc<OwnerError<C>>>>>,
}

#[derive(Debug)]
pub enum OwnerError<C> {
    Cleanup(CleanupErrors<C>),
    Panicked(DedicatedJoinError),
    PanickedWithCleanup {
        cause: DedicatedJoinError,
        cleanup: CleanupErrors<C>,
    },
    RuntimeStopped,
    Cancelled,
}
