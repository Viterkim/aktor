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
mod start;

/// The resource on its own thread, with a handle for calling your functions.
pub struct Aktor<S, Role = ()> {
    #[doc(hidden)]
    pub handle: Handle<S, Role>,
    #[doc(hidden)]
    pub control: Lifecycle,
    #[doc(hidden)]
    pub completion: OwnerCompletion,
}

/// Observe a dedicated actor’s cleanup and thread completion without keeping the actor alive.
pub struct OwnerCompletion {
    cleanup: crate::listener::lifecycle::CleanupReceipt<AktorFailure>,
    result: watch::Receiver<Option<Result<(), Arc<OwnerError>>>>,
}

/// A failed start for one native actor thread. Group rollback is on `AktorStartError`.
#[derive(er::Er)]
#[er(no_constructors, format = "{cause}")]
#[non_exhaustive]
pub struct StartError {
    /// What went wrong, match on this.
    pub kind: AktorStartKind,
    /// Whether the actor's thread is gone.
    pub thread: AktorThreadEnd,
    pub cause: crate::listener::DedicatedStartError<AktorFailure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorStartKind {
    /// Your setup returned an error. Its report comes with the start error.
    Setup,
    /// Your setup panicked.
    Panicked,
    /// What you passed in can't work.
    Invalid(AktorStartInvalid),
    /// The OS wouldn't give us a thread or a runtime.
    Spawn,
    /// The group is stopping.
    Stopping,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorStartInvalid {
    ZeroCapacity,
    NoRuntime,
    GroupNotStarted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorThreadEnd {
    NeverCreated,
    Joined,
    /// It might still be running.
    Unconfirmed,
}

pub enum OwnerError {
    Cleanup(CleanupErrors<AktorFailure>),
    CleanupUnconfirmed,
    Panicked(DedicatedJoinError),
    PanickedWithCleanup {
        cause: DedicatedJoinError,
        cleanup: CleanupErrors<AktorFailure>,
    },
    RuntimeStopped,
    Cancelled,
}

#[derive(Clone, Copy, PartialEq, Eq, er::Er)]
#[er(no_constructors)]
pub enum CleanupError {
    /// `end` returned an error. Its report travels alongside this value.
    #[er(format = "actor cleanup failed")]
    Failed,
    /// Aktor never received a result from `end`, for example after a panic or a timeout.
    #[er(format = "actor cleanup result is unconfirmed")]
    Unconfirmed,
}

#[doc(hidden)]
pub struct Lifecycle {
    inner: crate::listener::AktorControl<AktorFailure>,
}
