use super::{Dedicated, DedicatedStartError, Handle, Listener, SpawnArgs, channel};
use er::Er;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};

pub mod answer;
pub mod impls;
pub mod run;
pub mod spawn;
use answer::{ReplaceReply, ResumeReply, SetupAnswer};

pub struct Actor<S, E, C> {
    admission: Arc<crate::queue::Admission>,
    commands: mpsc::Sender<Command<S, E, C>>,
    force: watch::Sender<bool>,
    running: watch::Receiver<bool>,
    abandoned_setup: Arc<Mutex<Vec<AbandonedSetup<E>>>>,
    failed_cleanup: Arc<Mutex<FailedCleanup<C>>>,
}

pub struct FailedCleanup<C> {
    errors: Vec<Arc<C>>,
}

#[derive(Debug)]
pub enum AbandonedSetup<E> {
    Resume(E),
    Replace(E),
}

#[derive(Er)]
pub enum LifecycleError<E> {
    #[er(format = "actor stopped")]
    Closed,
    #[er(format = "actor is already running")]
    AlreadyRunning,
    #[er(format = "actor lifecycle operation failed: {0:?}")]
    Failed(#[er(source)] E),
}

#[derive(Er)]
pub enum ReplaceError<E, C> {
    #[er(format = "actor stopped")]
    Closed,
    #[er(format = "replacement cleanup failed: {0:?}")]
    Cleanup(#[er(source)] Arc<C>),
    #[er(format = "replacement setup failed: {0:?}")]
    Setup(#[er(source)] E),
}

#[derive(Debug)]
pub struct CleanupErrors<C> {
    pub errors: Vec<Arc<C>>,
}

pub type Reply<E> = oneshot::Sender<Result<(), LifecycleError<E>>>;

pub enum Command<S, E, C> {
    Pause(Reply<Arc<C>>),
    Resume {
        setup: Box<dyn FnOnce() -> crate::message::LocalFuture<'static, Result<S, E>> + Send>,
        reply: ResumeReply<E>,
    },
    Replace {
        setup: Box<dyn FnOnce() -> crate::message::LocalFuture<'static, Result<S, E>> + Send>,
        reply: ReplaceReply<E, C>,
    },
    Shutdown(Reply<Arc<C>>),
}

enum Event<S, E, C> {
    Command(Option<Command<S, E, C>>),
    Message(Option<crate::message::Message<S>>),
    NoHandles,
}
