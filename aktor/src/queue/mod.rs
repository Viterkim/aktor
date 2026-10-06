use parking_lot::ReentrantMutex;
use std::{
    cell::RefCell,
    sync::{Arc, Weak},
};
use tokio::sync::watch;

mod impls;
pub mod mailbox;

pub struct HandleInner<S> {
    pub sender: mailbox::Sender<S>,
    pub finished: watch::Receiver<()>,
    pub admission: Arc<Admission>,
    pub _alive: watch::Sender<()>,
}

pub struct Admission {
    standard: std::sync::atomic::AtomicBool,
    state: ReentrantMutex<RefCell<AdmissionState>>,
    changed: watch::Sender<u64>,
}

struct AdmissionState {
    open: bool,
    epoch: u64,
    group: Option<(String, crate::group::KillSwitch)>,
    phase: Phase,
    sessions: Vec<Weak<dyn Fn(Phase) -> bool + Send + Sync>>,
    prune_at: usize,
}

#[derive(Clone, Copy)]
pub enum Phase {
    Running,
    Paused,
    Closing { paused: bool },
}
