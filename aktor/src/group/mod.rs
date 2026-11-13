pub use crate::{AktorArgs, AktorFailureReport, AktorOutcome, AktorShutdownReport};
use crate::{AktorError, AktorExecution};
use core::{future::Future, pin::Pin};
#[cfg(not(target_family = "wasm"))]
use std::time::Instant;
#[cfg(target_family = "wasm")]
use std::{cell::RefCell, rc::Rc};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
use {crate::threads::lock::Mutex, signal as watch};
#[cfg(not(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
)))]
use {std::sync::Mutex, tokio::sync::watch};

#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
mod signal;

#[cfg(any(
    feature = "tokio",
    feature = "wasm_browser_workers",
    target_family = "wasm"
))]
mod application;
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser;
#[cfg(target_family = "wasm")]
mod clock;
mod impls;
#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
mod intervals;
#[cfg(not(target_family = "wasm"))]
mod native;
pub mod shutdown;

#[cfg(target_family = "wasm")]
#[doc(hidden)]
pub use clock::Instant;
#[doc(hidden)]
pub use impls::shutdown_deadline;

/// Aktors sharing one shutdown. Dropping a started group begins closing it.
pub struct AktorGroup {
    stop_on_drop: bool,
    control: Arc<Control>,
    startup_failure: Option<Arc<AtomicBool>>,
    #[cfg(not(target_family = "wasm"))]
    actors: Arc<Mutex<Vec<Entry>>>,
    #[cfg(target_family = "wasm")]
    actors: Rc<RefCell<Vec<Entry>>>,
    #[cfg(target_family = "wasm")]
    shutdown_hook: Rc<RefCell<Option<ShutdownHook>>>,
    /// The shared state stays Send, so the late collector lives here in the browser.
    #[cfg(target_family = "wasm")]
    late: Rc<RefCell<Option<Late>>>,
}

/// Put this in your Ctrl+C or Close handler. Clones stop the same group.
#[derive(Clone)]
pub struct AktorKillSwitch {
    control: Arc<Control>,
    startup_failure: Option<Arc<AtomicBool>>,
}

/// Get the shutdown report again later.
#[derive(Clone)]
pub struct AktorGroupCompletion {
    result: watch::Receiver<Option<Completed>>,
}

struct Completed {
    report: AktorShutdownReport,
    retirement_final: bool,
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
struct ApplicationLife {
    kill: AktorKillSwitch,
}

struct Control {
    stopping: AtomicBool,
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    runtime: Mutex<Option<tokio::runtime::Handle>>,
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    standard: std::sync::atomic::AtomicBool,
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    intervals: Mutex<Option<tokio::sync::mpsc::UnboundedSender<intervals::Pending>>>,
    state: Mutex<State>,
    changed: watch::Sender<Option<Instant>>,
    completed: watch::Sender<Option<Completed>>,
    grace: Duration,
    #[cfg(not(target_family = "wasm"))]
    shutdown_policy: crate::AktorShutdownPolicy,
    application_changed: watch::Sender<()>,
    #[cfg(not(target_family = "wasm"))]
    wake: std::sync::Condvar,
    #[cfg(not(target_family = "wasm"))]
    threads: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    starting: bool,
    report: AktorShutdownReport,
    listening: bool,
    deadline: Option<Instant>,
    finished: bool,
    #[cfg(not(target_family = "wasm"))]
    actors_and_hooks_done: bool,
    #[cfg(not(target_family = "wasm"))]
    callback_running: bool,
    #[cfg(not(target_family = "wasm"))]
    watchdog_disposal: WatchdogDisposal,
    #[cfg(not(target_family = "wasm"))]
    listener_hook: Option<ShutdownHook>,
    #[cfg(not(target_family = "wasm"))]
    shutdown_hook: Option<ShutdownHook>,
    kinds: Vec<(String, AktorExecution)>,
    #[cfg(not(target_family = "wasm"))]
    /// Each thread with its actor's `stuck`, the watchdog only waits for the `Kill` ones.
    running: Vec<(Arc<String>, crate::AktorStuck)>,
    applications: usize,
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    callers: Vec<tokio::task::JoinHandle<()>>,
    /// Keeps collecting outcomes of actors shutdown gave up on, after the report went out.
    #[cfg(not(target_family = "wasm"))]
    late: Option<Late>,
}

#[cfg(not(target_family = "wasm"))]
#[derive(Default, PartialEq, Eq)]
enum WatchdogDisposal {
    #[default]
    Available,
    Requested,
    Gone,
}

#[cfg(not(target_family = "wasm"))]
#[doc(hidden)]
pub struct ThreadLife {
    control: Arc<Control>,
    name: Arc<String>,
}

#[cfg(not(target_family = "wasm"))]
type Late = Pin<Box<dyn Future<Output = ()> + Send>>;
#[cfg(target_family = "wasm")]
type Late = Pin<Box<dyn Future<Output = ()>>>;

#[cfg(not(target_family = "wasm"))]
type CloseFuture = Pin<Box<dyn Future<Output = AktorOutcome> + Send>>;
#[cfg(target_family = "wasm")]
type CloseFuture = Pin<Box<dyn Future<Output = AktorOutcome>>>;

#[cfg(not(target_family = "wasm"))]
type Action = Box<dyn FnOnce() + Send>;
#[cfg(target_family = "wasm")]
type Action = Box<dyn FnOnce()>;

/// Final status and diagnostics when an unconfirmed owner retires.
#[cfg(not(target_family = "wasm"))]
type ConfirmLater =
    Pin<Box<dyn Future<Output = Option<(crate::AktorRetireStatus, Vec<AktorError>)>> + Send>>;
#[cfg(target_family = "wasm")]
type ConfirmLater =
    Pin<Box<dyn Future<Output = Option<(crate::AktorRetireStatus, Vec<AktorError>)>>>>;

/// Returns whether the owner is gone for sure afterwards, like a terminated browser worker.
#[cfg(not(target_family = "wasm"))]
type GiveUp = Box<dyn FnOnce() -> bool + Send>;
#[cfg(target_family = "wasm")]
type GiveUp = Box<dyn FnOnce() -> bool>;

#[cfg(not(target_family = "wasm"))]
type ShutdownFuture = Pin<Box<dyn Future<Output = Result<(), AktorError>> + Send>>;
#[cfg(target_family = "wasm")]
type ShutdownFuture = Pin<Box<dyn Future<Output = Result<(), AktorError>>>>;

#[cfg(not(target_family = "wasm"))]
type ShutdownHook = Box<dyn FnOnce(AktorShutdownReport) -> ShutdownFuture + Send>;
#[cfg(target_family = "wasm")]
type ShutdownHook = Box<dyn FnOnce(AktorShutdownReport) -> ShutdownFuture>;

struct Entry {
    name: String,
    kind: AktorExecution,
    start: Action,
    cancel: Action,
    /// Runs when shutdown gives up on the owner, for owners that can be stopped from outside.
    give_up: Option<GiveUp>,
    /// Only browser workers, their retained work can confirm after the report went out.
    confirm_later: Option<ConfirmLater>,
    outcome: CloseFuture,
}
