pub mod latest;
pub mod mailbox;

mod impls;

#[cfg(any(not(target_family = "wasm"), feature = "bevy"))]
pub use impls::{channel, closed_channel};

use parking_lot::ReentrantMutex;
use std::{
    cell::RefCell,
    sync::{Arc, Weak},
};
use tokio::sync::watch;

pub struct HandleInner<M> {
    pub sender: mailbox::Sender<M>,
    pub(crate) jobs: Weak<crate::owner_tasks::External>,
    /// Turns `true` when the actor failed, and closes once it's gone.
    pub finished: watch::Receiver<bool>,
    pub admission: Arc<Admission>,
}

pub struct Mailbox<M> {
    pub receiver: mailbox::Receiver<M>,
    pub(crate) jobs: Arc<crate::owner_tasks::External>,
    pub admission: Arc<Admission>,
    /// Dropped once the owner let go of everything, `finished` on the handles waits for it.
    pub finished: watch::Sender<bool>,
}

pub struct Admission {
    pub(crate) lost: std::sync::OnceLock<crate::Delivery>,
    pub(crate) terminal: std::sync::OnceLock<crate::Delivery>,
    permits: Arc<tokio::sync::Semaphore>,
    job_capacity: usize,
    #[cfg(any(feature = "tokio", not(target_family = "wasm")))]
    standard: std::sync::atomic::AtomicBool,
    state: ReentrantMutex<RefCell<AdmissionState>>,
    changed: watch::Sender<()>,
}

struct AdmissionState {
    refused: Option<crate::Delivery>,
    group: Option<crate::group::AktorKillSwitch>,
    phase: Phase,
    jobs: std::collections::BTreeMap<usize, Arc<crate::owner_tasks::Shared>>,
    sessions: Vec<Weak<dyn Fn(Phase) -> bool + Send + Sync>>,
    prune_at: usize,
    callbacks: std::collections::BTreeMap<usize, Box<dyn FnOnce() + Send>>,
    next_callback: usize,
}

/// A message that can find itself in the queue again, so a latest session can take back what it
/// scheduled.
pub trait Service {
    fn service_id(&self) -> Option<*const ()>;
}

#[derive(Clone, Copy)]
pub enum Phase {
    Running,
    Closing,
}

pub(crate) struct JobPermit {
    admission: Arc<Admission>,
    id: usize,
}
impl Drop for JobPermit {
    fn drop(&mut self) {
        self.admission
            .state
            .lock()
            .borrow_mut()
            .jobs
            .remove(&self.id);
    }
}
