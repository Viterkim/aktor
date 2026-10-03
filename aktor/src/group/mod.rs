pub use crate::lifecycle::{ActorArgs, ActorFailure, ActorOutcome, ShutdownReport};
use crate::{AktorCleanupError, AktorError};
use core::{future::Future, pin::Pin};
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

/// Keep the application and its actors together.
pub struct AktorGroup {
    control: Arc<Control>,
    actors: Vec<Entry>,
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
