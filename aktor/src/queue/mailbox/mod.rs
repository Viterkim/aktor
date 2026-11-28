mod impls;

pub use impls::channel;

use core::task::Waker;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Semaphore, SemaphorePermit, mpsc};

struct Queue<M> {
    entries: Mutex<Entries<M>>,
    permits: Arc<Semaphore>,
    senders_closed: AtomicBool,
}

struct Entries<M> {
    receiving: bool,
    /// Each with whether it holds a queue slot, calls do and services don't.
    messages: VecDeque<(M, bool)>,
    /// The owner waiting for a message. Kept with the messages, so one lock pops or registers.
    waiting: Option<Waker>,
    /// Something besides a message wants the owner's attention, like a stop or a force.
    nudged: bool,
    /// The owner in the middle of a call, waiting only for a nudge.
    watching: Option<Waker>,
}

/// What the owner finds in its mailbox.
pub enum Received<M> {
    Message(M),
    Closed,
    /// A stop or a force, the owner looks at those again.
    Nudged,
}

/// Wakes the owner once the lock that pushed the message is gone.
#[must_use]
pub struct Ready(Option<Waker>);

pub struct Sender<M>(Arc<Queue<M>>);

pub struct Receiver<M>(Arc<Queue<M>>);

pub struct Permit<'a, M> {
    queue: &'a Queue<M>,
    permit: SemaphorePermit<'a>,
}
