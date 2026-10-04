pub use crate::lifecycle::{ActorArgs, ActorFailure, ActorOutcome, ShutdownReport};
use crate::{AktorCleanupError, AktorError};
use core::{future::Future, pin::Pin};
#[cfg(target_family = "wasm")]
use std::{cell::RefCell, rc::Rc};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

mod impls;
#[cfg(not(target_family = "wasm"))]
use std::time::Instant;
#[cfg(target_family = "wasm")]
mod clock;
#[cfg(target_family = "wasm")]
use clock::Instant;
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser;
#[cfg(not(target_family = "wasm"))]
mod native;

/// Actors sharing one shutdown. Dropping a started group begins closing it.
pub struct AktorGroup {
    stop_on_drop: bool,
    control: Arc<Control>,
    #[cfg(not(target_family = "wasm"))]
    actors: Arc<Mutex<Vec<Entry>>>,
    #[cfg(target_family = "wasm")]
    actors: Rc<RefCell<Vec<Entry>>>,
}

/// Put this in your Ctrl+C or Close handler. Clones stop the same group.
#[derive(Clone)]
pub struct KillSwitch {
    control: Arc<Control>,
}

/// Get the shutdown report again later.
pub struct GroupCompletion {
    result: watch::Receiver<Option<ShutdownReport>>,
}

struct Control {
    state: Mutex<State>,
    changed: watch::Sender<Option<Instant>>,
    completed: watch::Sender<Option<ShutdownReport>>,
    grace: Duration,
    #[cfg(not(target_family = "wasm"))]
    wake: std::sync::Condvar,
    #[cfg(not(target_family = "wasm"))]
    threads: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    report: ShutdownReport,
    listening: bool,
    finished: bool,
    force_exit: bool,
    #[cfg(not(target_family = "wasm"))]
    running: Vec<Arc<String>>,
}

#[cfg(not(target_family = "wasm"))]
#[doc(hidden)]
pub struct ThreadLife {
    control: Arc<Control>,
    name: Arc<String>,
}

#[cfg(not(target_family = "wasm"))]
type CloseFuture = Pin<Box<dyn Future<Output = ActorOutcome> + Send>>;
#[cfg(target_family = "wasm")]
type CloseFuture = Pin<Box<dyn Future<Output = ActorOutcome>>>;

#[cfg(not(target_family = "wasm"))]
type Action = Box<dyn FnOnce() + Send>;
#[cfg(target_family = "wasm")]
type Action = Box<dyn FnOnce()>;

struct Entry {
    name: String,
    start: Action,
    cancel: Action,
    outcome: CloseFuture,
}
