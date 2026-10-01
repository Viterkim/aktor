use super::{Dedicated, DedicatedStartError, Handle, Listener, SpawnArgs, channel};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};

pub mod answer;
pub mod impls;
pub mod run;
pub mod spawn;
use answer::{ReplaceReply, ResumeReply, SetupAnswer};

pub struct Actor<S, E, C> {
    commands: mpsc::Sender<Command<S, E, C>>,
    running: watch::Receiver<bool>,
    abandoned_setup: Arc<Mutex<Vec<AbandonedSetup<E>>>>,
}

#[derive(Debug)]
pub enum AbandonedSetup<E> {
    Resume(E),
    Replace(E),
}

#[derive(Debug)]
pub enum LifecycleError<E> {
    Closed,
    AlreadyRunning,
    Failed(E),
}

#[derive(Debug)]
pub enum ReplaceError<E, C> {
    Closed,
    Cleanup(Arc<C>),
    Setup(E),
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
