//! Thread actors block on the mailbox loop on their own thread. Task actors spawn it.

use crate::{
    AktorCleanupError, AktorExecution, AktorKillSwitch, AktorStuck, OwnerOutcome,
    operation::{Operation, hooks::CallHooks},
    queue::{HandleInner, Mailbox},
};
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{any::Any, future::Future, time::Duration};
use er::*;
use futures_util::task::AtomicWaker;
use std::sync::{Arc, Weak, atomic::AtomicBool};
use tokio::sync::{mpsc, watch};

pub mod clock;
pub(crate) mod impls;
mod intervals;
mod register;
mod run;

pub use clock::{Clock, Deadline};
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
pub use register::{Join, TaskJoin};
pub(crate) use register::{Registered, register};
pub(crate) use run::serve;

pub trait Mail<S>: Sized {
    type State;

    fn operation(&self) -> Operation;

    fn run<'a>(
        &'a mut self,
        state: &'a mut Self::State,
        hooks: &'a mut CallHooks<S>,
    ) -> impl Future<Output = ()> + 'a;
}

pub(crate) struct Serve<S, M, Start, End, Ready> {
    pub name: String,
    pub kind: AktorExecution,
    pub kill: AktorKillSwitch,
    pub mailbox: Mailbox<M>,
    /// Intervals post their messages through it, without keeping the actor alive.
    pub handle: Weak<HandleInner<M>>,
    pub force: watch::Receiver<bool>,
    pub start: Start,
    pub end: End,
    pub hooks: CallHooks<S>,
    pub intervals: Vec<Interval<M>>,
    pub stuck: AktorStuck,
    pub clock: Clock,
    /// Give the executor a turn every few calls, for hosts that share their threads with others.
    pub yields: bool,
    pub ready: Ready,
    /// What `end` returned, as soon as it's back.
    pub cleaned: watch::Sender<Option<crate::serve::EndReceipt>>,
    pub report: Report,
}

#[derive(Clone, Copy, PartialEq, Eq, Er)]

pub enum CleanupError {
    /// `end` returned an error. Its report goes into the group's shutdown report.
    #[er(format = "actor cleanup failed")]
    Failed(#[er(skip)] crate::FailureId),
    /// Aktor never received a result from `end`, for example after a panic or a timeout.
    #[er(format = "actor cleanup result is unconfirmed")]
    Unconfirmed(#[er(skip)] Option<crate::FailureId>),
}

pub(crate) type EndReceipt = Result<(), (CleanupError, crate::Delivery, String)>;

pub(crate) enum Started<E> {
    Ready,
    Init(ErTree<crate::LaunchError<E>>),
    Cut,
}

pub(crate) struct Served {
    pub returned: Vec<crate::Delivery>,
    pub owner_loss: Option<crate::FailureId>,
    pub outcome: OwnerOutcome,
    /// The first panic, kept for a thread's completion.
    pub panic: Option<Box<dyn Any + Send>>,
    pub later: Vec<ErBranch<impls::ServePanic>>,
    /// What `end` returned, `None` when it never got that far.
    pub cleanup: Option<Result<(), String>>,
    /// Shutdown cut a call or threw admitted calls away.
    pub cut: bool,
}

/// An interval turns into a message each time it's due. The message carries `Due` and drops it
/// once it ran or got thrown away, then the next wait starts.
pub struct Interval<M> {
    pub every: Duration,
    pub message: Box<dyn FnMut(Due) -> M + Send>,
}

pub struct Due(Arc<DueState>);

struct DueState {
    done: AtomicBool,
    waker: AtomicWaker,
}

/// Dropping an unfinished report tells the group the actor was cancelled.
pub(crate) struct Report {
    admission: Arc<crate::queue::Admission>,
    served: Option<mpsc::UnboundedSender<Served>>,
    returned: Vec<crate::Delivery>,
    kill: AktorKillSwitch,
    name: String,
    kind: AktorExecution,
    outcome: OwnerOutcome,
    panic: Option<Box<dyn Any + Send>>,
    later: Vec<ErBranch<impls::ServePanic>>,
    cleanup: Option<Result<(), String>>,
    cut: bool,
    /// Something went wrong in this actor, not just somewhere in its group.
    failed: bool,
}
