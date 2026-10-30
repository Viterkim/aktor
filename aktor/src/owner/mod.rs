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
    pub handle: Handle<S, Role>,
    pub control: Lifecycle<S>,
    pub completion: OwnerCompletion,
}

/// Wait for the result without keeping the actor alive.
pub struct OwnerCompletion {
    result: watch::Receiver<Option<Result<(), Arc<OwnerError>>>>,
}

#[derive(er::Er)]
#[er(no_constructors, format = "{error}", display_alt = Self::fmt_details)]
pub struct StartError {
    pub error: crate::listener::DedicatedStartError<AktorFailure>,
    #[er(source, skip)]
    pub diagnostics: Option<crate::AktorDiagnostic>,
}

#[derive(Debug)]
pub enum OwnerFailure {
    Cleanup(CleanupErrors<AktorFailure>),
    Panicked(DedicatedJoinError),
    PanickedWithCleanup {
        cause: DedicatedJoinError,
        cleanup: CleanupErrors<AktorFailure>,
    },
    RuntimeStopped,
    Cancelled,
}

#[derive(er::Er)]
#[er(no_constructors, format = "{error}", display_alt = Self::fmt_details)]
pub struct OwnerError {
    pub error: OwnerFailure,
    #[er(source, skip)]
    pub diagnostics: crate::AktorDiagnostic,
}

#[derive(er::Er)]
#[er(no_constructors, format = "{error}", display_alt = Self::fmt_details)]
pub struct LifecycleError<E = crate::AktorSetupError> {
    pub error: crate::listener::LifecycleError<E>,
    #[er(source, skip)]
    pub diagnostics: Option<crate::AktorDiagnostic>,
}

#[derive(er::Er)]
#[er(no_constructors, format = "{error}", display_alt = Self::fmt_details)]
pub struct ReplaceError {
    pub error: crate::listener::ReplaceError,
    #[er(source, skip)]
    pub diagnostics: Option<crate::AktorDiagnostic>,
}

#[derive(Debug)]
struct CompletionScheduleError {
    cause: std::io::Error,
    cleanup: Arc<OwnerError>,
}

pub struct Lifecycle<S> {
    inner: crate::listener::AktorControl<S, crate::AktorSetupError, AktorFailure>,
}
