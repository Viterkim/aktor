use super::*;
pub use crate::{AktorArgs, AktorShutdownReport};
use crate::{AktorError, AktorFailureReport, AktorOutcome, AktorRetireStatus};
use alloc::{string::String, vec::Vec};
use clock::AktorGroupClock;
use core::{future::poll_fn, ops::AsyncFnOnce, time::Duration};
#[cfg(feature = "embassy_cross_core")]
use portable_atomic::{AtomicBool, Ordering};
#[cfg(feature = "embassy_cross_core")]
use portable_atomic_util::Arc as Shared;

mod driver;
mod impls;
mod shutdown;

pub struct AktorGroup<Clock: AktorGroupClock> {
    stop_on_drop: bool,
    control: Rc<Control<Clock>>,
    startup_failure: Option<Rc<Cell<bool>>>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

pub struct AktorGroupCompletion<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
}

pub struct AktorKillSwitch<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
    startup_failure: Option<Rc<Cell<bool>>>,
}

struct Control<Clock: AktorGroupClock> {
    stopping: Cell<bool>,
    starting: Cell<bool>,
    #[cfg(feature = "embassy_cross_core")]
    shared_stopping: Shared<AtomicBool>,
    listening: Cell<bool>,
    shutdown_hook: RefCell<Option<ShutdownHook>>,
    callers: RefCell<Vec<LocalFuture<'static, ()>>>,
    grace: Duration,
    deadline: Cell<Option<Clock::Deadline>>,
    kinds: RefCell<Vec<(String, crate::AktorExecution)>>,
    failure: RefCell<Option<AktorFailureReport>>,
    completed: RefCell<Option<AktorShutdownReport>>,
    report: RefCell<AktorShutdownReport>,
    changed: Event,
}

type ShutdownHook =
    Box<dyn FnOnce(AktorShutdownReport) -> LocalFuture<'static, Result<(), AktorError>>>;

struct Driver<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

#[derive(Default)]
pub(crate) struct OwnerStatus {
    pub cleanup_completed: Option<bool>,
    pub ran_past_grace: bool,
    pub panic_reported: bool,
    pub cleanup_diagnostic: Option<AktorError>,
}

struct Entry {
    status: Box<dyn Fn() -> OwnerStatus>,
    name: String,
    kind: crate::AktorExecution,
    shutdown: Box<dyn Fn()>,
    /// Drops the running calls once the actor's grace is up, `end` still runs.
    cancel: Option<Box<dyn Fn()>>,
    owner: LocalFuture<'static, Option<AktorError>>,
    diagnostics: Rc<RefCell<Vec<AktorError>>>,
    finished: bool,
}
