pub use crate::{ActorArgs, ActorFailure, ActorOutcome, ShutdownReport};
use crate::{AktorCleanupError, AktorError, AktorExecution};
use core::{future::Future, pin::Pin};
#[cfg(target_family = "wasm")]
use std::{cell::RefCell, rc::Rc};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;

mod impls;
#[doc(hidden)]
pub use impls::shutdown_deadline;
#[cfg(any(
    any(feature = "tokio", feature = "wasm_browser_workers"),
    target_family = "wasm"
))]
mod run;
pub mod shutdown;
#[cfg(not(target_family = "wasm"))]
use std::time::Instant;
#[cfg(target_family = "wasm")]
mod clock;
#[cfg(target_family = "wasm")]
#[doc(hidden)]
pub use clock::Instant;
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
#[derive(Clone)]
pub struct GroupCompletion {
    result: watch::Receiver<Option<ShutdownReport>>,
}

struct Control {
    stopping: AtomicBool,
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    runtime: Mutex<Option<tokio::runtime::Handle>>,
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    standard: std::sync::atomic::AtomicBool,
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
    deadline: Option<Instant>,
    finished: bool,
    force_exit: bool,
    kinds: Vec<(String, AktorExecution)>,
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
    kind: AktorExecution,
    start: Action,
    cancel: Action,
    outcome: CloseFuture,
}
