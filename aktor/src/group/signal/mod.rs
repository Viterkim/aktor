mod impls;
pub use impls::channel;

use crate::{
    cross_core::event::{Event, Listener},
    threads::lock::Mutex,
};
use portable_atomic_util::Arc;

struct Shared<T> {
    state: Mutex<State<T>>,
    changed: Arc<Event>,
}

struct State<T> {
    value: T,
    revision: u64,
    closed: bool,
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    listener: Listener,
    revision: u64,
}
