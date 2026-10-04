use crate::message::Message;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Notify, Semaphore, SemaphorePermit, mpsc};

mod impls;
pub use impls::channel;

struct Queue<S> {
    entries: Mutex<Entries<S>>,
    permits: Semaphore,
    ready: Notify,
    senders_closed: AtomicBool,
}

struct Entries<S> {
    receiving: bool,
    messages: VecDeque<Message<S>>,
}

pub struct Sender<S>(Arc<Queue<S>>);

pub struct Receiver<S>(Arc<Queue<S>>);

pub struct Permit<'a, S> {
    queue: &'a Queue<S>,
    permit: SemaphorePermit<'a>,
}
