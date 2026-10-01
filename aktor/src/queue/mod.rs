use parking_lot::ReentrantMutex;
use std::{cell::RefCell, sync::Arc};
use tokio::sync::watch;

mod impls;
pub mod key;
pub mod mailbox;

pub struct HandleInner<S> {
    pub sender: mailbox::Sender<S>,
    pub finished: watch::Receiver<()>,
    pub admission: Arc<Admission>,
    pub _alive: watch::Sender<()>,
}

pub struct Admission {
    state: ReentrantMutex<RefCell<AdmissionState>>,
    changed: watch::Sender<u64>,
}

struct AdmissionState {
    open: bool,
    epoch: u64,
}
