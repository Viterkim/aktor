use crate::{message::Message, queue::key::LatestKey};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Notify, Semaphore, SemaphorePermit, mpsc, watch};

mod impls;
pub use impls::channel;

struct Queue<S> {
    entries: Mutex<Entries<S>>,
    permits: Semaphore,
    ready: Notify,
    changed: watch::Sender<()>,
    senders_closed: AtomicBool,
}

struct Entries<S> {
    messages: VecDeque<Message<S>>,
    revision: u64,
}

pub struct Snapshot {
    revision: u64,
    pub key: Option<LatestKey>,
}

pub struct Sender<S>(Arc<Queue<S>>);

pub struct Receiver<S>(Arc<Queue<S>>);

pub struct Permit<'a, S> {
    queue: &'a Queue<S>,
    permit: SemaphorePermit<'a>,
}
