use crate::{
    AktorFailure,
    listener::{CleanupErrors, DedicatedJoinError, Handle},
};
use std::sync::Arc;
use tokio::sync::watch;

mod dispatch;
mod impls;
mod lifecycle;
mod opaque_dispatch;

/// The resource on its own thread, with a handle for calling your functions.
pub struct Aktor<S, Role = ()> {
    pub handle: Handle<S, Role>,
    pub actor: Lifecycle<S>,
    pub completion: OwnerCompletion,
}

/// Wait for the result without keeping the actor alive.
pub struct OwnerCompletion {
    result: watch::Receiver<Option<Result<(), Arc<OwnerError>>>>,
}

#[derive(Debug)]
pub enum OwnerError {
    Cleanup(CleanupErrors<AktorFailure>),
    Panicked(DedicatedJoinError),
    PanickedWithCleanup {
        cause: DedicatedJoinError,
        cleanup: CleanupErrors<AktorFailure>,
    },
    RuntimeStopped,
    Cancelled,
}

#[derive(Debug)]
struct CompletionScheduleError {
    cause: std::io::Error,
    cleanup: Arc<OwnerError>,
}

pub struct Lifecycle<S> {
    inner: crate::listener::Actor<S, crate::AktorSetupError, AktorFailure>,
}
