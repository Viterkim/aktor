mod dispatch;
mod impls;
mod opaque_dispatch;
#[cfg(all(test, feature = "tokio"))]
mod tests;
mod thread;

pub use crate::serve::CleanupError;
pub(crate) use thread::{Launched, launch};

use crate::listener::{Dedicated, DedicatedJoinError, Handle};
use er::*;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

/// The resource on its own thread, with a handle for calling your functions.
pub struct Aktor<S, Role = ()> {
    #[doc(hidden)]
    pub handle: Handle<S, Role>,
    #[doc(hidden)]
    pub completion: OwnerCompletion,
    #[doc(hidden)]
    pub kill: crate::AktorKillSwitch,
}

/// Observe a dedicated actor’s cleanup and thread completion without keeping the actor alive.
pub struct OwnerCompletion {
    cleanup: watch::Receiver<Option<crate::serve::EndReceipt>>,
    result: watch::Receiver<Option<(CompletionResult, Vec<crate::Delivery>)>>,
}

/// What joins an owner thread once its actor is done, then publishes the actor's completion and
/// tells a failed start how its thread ended.
struct ThreadJoin {
    thread: Dedicated<()>,
    spawner: crate::executor::Spawner,
    completed: watch::Sender<Option<(CompletionResult, Vec<crate::Delivery>)>>,
    startup: oneshot::Sender<Result<(), (crate::FailureId, String)>>,
}

type CompletionResult = Result<(), (Arc<OwnerError>, String)>;

#[derive(Er)]

pub enum OwnerError {
    Cleanup,
    CleanupUnconfirmed,
    Panicked(#[er(skip)] Arc<DedicatedJoinError>),
    PanickedWithCleanup {
        #[er(skip)]
        cause: Arc<DedicatedJoinError>,
    },
    RuntimeStopped,
    Cancelled,
}
