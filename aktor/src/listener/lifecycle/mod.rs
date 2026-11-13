use super::{Dedicated, DedicatedStartError, Handle, Listener, SpawnArgs, channel};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

pub mod impls;
pub mod run;
pub mod spawn;
#[cfg(not(target_family = "wasm"))]
pub(crate) use impls::control::wait_cleanup;

#[doc(hidden)]
pub struct AktorControl<C> {
    admission: Arc<crate::queue::Admission>,
    force: watch::Sender<bool>,
    shutdown: watch::Sender<bool>,
    running: watch::Receiver<bool>,
    failed_cleanup: Arc<Mutex<FailedCleanup<C>>>,
    cleanup: CleanupReceipt<C>,
}

pub(crate) type CleanupReceipt<C> = watch::Receiver<Option<Result<(), Arc<C>>>>;

pub struct FailedCleanup<C> {
    /// `end` used up its grace and got dropped.
    ran_out: bool,
    /// Shutdown's cancel dropped a call that was still running.
    cut_call: bool,
    errors: Vec<Arc<C>>,
    diagnostics: Vec<crate::AktorError>,
    group_primary: bool,
}

#[derive(Debug)]
pub struct CleanupErrors<C> {
    pub errors: Vec<Arc<C>>,
    pub diagnostics: Vec<crate::AktorError>,
}

#[derive(Debug)]
pub enum CleanupError<C> {
    Returned(Arc<C>),
    Unconfirmed,
}

enum Event<S> {
    Shutdown,
    Message(Option<crate::message::Message<S>>),
}
