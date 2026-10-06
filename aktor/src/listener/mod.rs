use crate::message::Message;
use crate::queue::{Admission, HandleInner, mailbox};
use core::marker::PhantomData;
use er::Er;
use std::{
    io,
    sync::{Arc, Weak},
    thread,
};
use tokio::sync::{mpsc, watch};

pub mod failure;
pub use crate::operation::hooks;
mod impls;
#[doc(hidden)]
pub mod lifecycle;
mod running;
mod spawn;
mod teardown;
#[cfg(test)]
mod tests;

use failure::Failures;
pub use failure::{Failure, FailureKind, FailurePolicy, Operation, RunError};
pub use lifecycle::spawn::{spawn, spawn_async, spawn_async_with_hooks};
pub use lifecycle::{AbandonedSetup, Actor, CleanupErrors, LifecycleError, ReplaceError};
pub use spawn::channel;
#[cfg(feature = "tokio")]
use spawn::startup_cause;
#[cfg(feature = "tokio")]
pub use spawn::{
    spawn_local, spawn_local_with_policy, spawn_runner, spawn_thread, spawn_thread_with_policy,
};

/// Setup and cleanup run on the actor thread.
pub struct SpawnArgs<Setup, Cleanup> {
    pub name: String,
    pub capacity: usize,
    pub failure: FailurePolicy,
    pub setup: Setup,
    pub cleanup: Cleanup,
}

pub struct Handle<S, Role = ()> {
    // Submission needs this across modules; callers must go through requests.
    pub(crate) inner: Arc<HandleInner<S>>,
    role: PhantomData<fn() -> Role>,
}

pub struct WeakHandle<S, Role = ()> {
    inner: Weak<HandleInner<S>>,
    role: PhantomData<fn() -> Role>,
}

pub struct Listener<S> {
    pub name: String,
    pub hooks: hooks::AktorHooks<S>,
    receiver: mailbox::Receiver<S>,
    pub failure: FailurePolicy,
    admission: Arc<Admission>,
    handles: watch::Receiver<()>,
    // Held until cleanup and the queued messages have been dropped.
    _finished: watch::Sender<()>,
}

pub struct Dedicated<S> {
    thread: thread::JoinHandle<Option<S>>,
    finished: watch::Receiver<()>,
}

#[derive(Clone)]
pub struct CompletionObserver {
    finished: watch::Receiver<()>,
}

#[derive(Debug)]
pub struct DedicatedJoinError {
    pub payload: parking_lot::Mutex<Box<dyn std::any::Any + Send>>,
}

#[derive(Er)]
pub enum DedicatedStartError<E> {
    #[er(format = "start the actor group before spawning actors")]
    NotStarted,
    #[er(format = "actor group is closing")]
    Closed,
    #[er(format = "an owned actor needs a Tokio runtime")]
    NoRuntime,
    #[er(format = "actor capacity is outside Tokio's supported range")]
    InvalidCapacity,
    #[er(format = "could not spawn actor thread: {0}")]
    Thread(#[er(source)] io::Error),
    #[er(format = "could not initialize actor: {0:?}")]
    Init(#[er(source)] E),
    #[er(format = "actor {actor:?} panicked while initializing: {cause}")]
    Panicked {
        actor: String,
        #[er(source)]
        cause: DedicatedJoinError,
    },
}
