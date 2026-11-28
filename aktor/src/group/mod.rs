#[cfg(not(target_family = "wasm"))]
pub mod shared;
pub mod shutdown;

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod application;
#[cfg(target_family = "wasm")]
mod clock;
mod impls;
#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
mod signal;

pub use crate::{AktorArgs, AktorFailure, AktorFailureInput, AktorOutcome, AktorShutdownReport};
#[cfg(target_family = "wasm")]
#[doc(hidden)]
pub use clock::Instant;
#[doc(hidden)]
#[cfg(any(
    all(
        feature = "local",
        any(
            all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")),
            all(
                feature = "browser_local",
                target_family = "wasm",
                target_os = "unknown"
            )
        )
    ),
    all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ),
    all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_os = "unknown",
        target_feature = "atomics"
    )
))]
pub(crate) use impls::keeper;
pub(crate) use impls::shutdown_deadline;

use crate::{AktorCleanupError, AktorExecution, FailureId, OwnerOutcome, StopCause};
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

/// Aktors sharing one shutdown. Dropping a started group begins closing it.
pub struct AktorGroup {
    stop_on_drop: bool,
    control: Arc<Control>,
    startup_failure: Vec<(crate::ActorId, Arc<std::sync::OnceLock<FailureId>>)>,
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

/// Finishes once the group starts closing. It owns what it waits on, so a struct can keep it
/// and poll it by hand.
pub struct AktorGroupStopping(Pin<Box<dyn Future<Output = ()> + Send>>);

enum RunStopping {
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    None,
    Stopped,
    Waiting(AktorGroupStopping),
}

pin_project_lite::pin_project! {
    /// Future returned by `run`. Finishes with `None` if the group starts closing first.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct AktorRun<F> {
        stopping: RunStopping,
        #[pin]
        work: F,
    }
}

/// Put this in your Ctrl+C or Close handler. Clones stop the same group.
#[derive(Clone)]
pub struct AktorKillSwitch {
    control: Arc<Control>,
    startup_failure: Vec<(crate::ActorId, Arc<std::sync::OnceLock<FailureId>>)>,
}

/// Get the shutdown report again later.
#[derive(Clone)]
pub struct AktorGroupCompletion {
    result: watch::Receiver<Option<Completed>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Collection {
    Pending,
    Complete,
    Lost,
}

struct Completed {
    grace: Arc<AktorShutdownReport>,
    report: Arc<AktorShutdownReport>,
    collection: Collection,
    work_finished: bool,
}

struct Control {
    stopping: AtomicBool,
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    runtime: Mutex<Option<tokio::runtime::Handle>>,
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    standard: std::sync::atomic::AtomicBool,
    state: Mutex<State>,
    changed: watch::Sender<Option<Instant>>,
    completed: watch::Sender<Option<Completed>>,
    grace: Duration,
    #[cfg(not(target_family = "wasm"))]
    shutdown_policy: crate::AktorGroupStuck,
    application_changed: watch::Sender<()>,
    #[cfg(not(target_family = "wasm"))]
    threads: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    report: crate::Recorder,
    listening: bool,
    deadline: Option<Instant>,
    finished: bool,
    hook_at_grace: bool,
    #[cfg(not(target_family = "wasm"))]
    actors_and_hooks_done: bool,
    #[cfg(not(target_family = "wasm"))]
    callback_running: bool,
    callback_workers: usize,
    #[cfg(not(target_family = "wasm"))]
    watchdog_disposal: WatchdogDisposal,
    #[cfg(not(target_family = "wasm"))]
    listener_hook: Option<ShutdownHook>,
    #[cfg(not(target_family = "wasm"))]
    shutdown_hook: Option<ShutdownHook>,
    registered: usize,
    /// A handle to every actor, so no handle drop closes a mailbox. The group owns its actors.
    kept: Option<Vec<Box<dyn core::any::Any + Send>>>,
    #[cfg(not(target_family = "wasm"))]
    /// Each thread with its actor's `stuck`, the watchdog only waits for the `Kill` ones.
    running: Vec<(Arc<String>, crate::AktorStuck)>,
    applications: usize,
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    callers: Vec<application::Application>,
    /// Finished tasks get swept out when `callers` reaches this, not on every spawn.
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    prune_callers_at: usize,
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
type CloseFuture = Pin<Box<dyn Future<Output = OwnerOutcome> + Send>>;
#[cfg(target_family = "wasm")]
type CloseFuture = Pin<Box<dyn Future<Output = OwnerOutcome>>>;

#[cfg(not(target_family = "wasm"))]
type Action = Box<dyn FnOnce() + Send>;
#[cfg(target_family = "wasm")]
type Action = Box<dyn FnOnce()>;

/// Final status and diagnostics when an unconfirmed owner retires.
#[cfg(not(target_family = "wasm"))]
type ConfirmLater = Pin<Box<dyn Future<Output = Option<crate::AktorRetirement>> + Send>>;
#[cfg(target_family = "wasm")]
type ConfirmLater = Pin<Box<dyn Future<Output = Option<crate::AktorRetirement>>>>;

/// Returns whether the owner is gone for sure afterwards, like a terminated browser worker.
#[cfg(not(target_family = "wasm"))]
type GiveUp = Box<dyn FnOnce() -> bool + Send>;
#[cfg(target_family = "wasm")]
type GiveUp = Box<dyn FnOnce() -> bool>;

#[cfg(not(target_family = "wasm"))]
type ShutdownFuture = Pin<Box<dyn Future<Output = Result<(), (AktorCleanupError, String)>> + Send>>;
#[cfg(target_family = "wasm")]
type ShutdownFuture = Pin<Box<dyn Future<Output = Result<(), (AktorCleanupError, String)>>>>;

#[cfg(not(target_family = "wasm"))]
type ShutdownHook = Box<dyn FnOnce(AktorShutdownReport, AktorKillSwitch) -> ShutdownFuture + Send>;
#[cfg(target_family = "wasm")]
type ShutdownHook = Box<dyn FnOnce(AktorShutdownReport) -> ShutdownFuture>;

struct Entry {
    id: crate::ActorId,
    start: Action,
    cancel: Action,
    /// Runs when shutdown gives up on the owner, for owners that can be stopped from outside.
    give_up: Option<GiveUp>,
    /// Only browser workers, their retained work can confirm after the report went out.
    confirm_later: Option<ConfirmLater>,
    outcome: CloseFuture,
}
