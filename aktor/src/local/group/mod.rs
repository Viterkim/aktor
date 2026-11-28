mod driver;
mod impls;
mod shutdown;

pub use crate::{AktorArgs, AktorShutdownReport};

use super::*;
use crate::{
    AktorCleanupError, AktorFailureInput, AktorRetirement, FailureId, OwnerOutcome, StopCause,
};
use alloc::{string::String, vec::Vec};
use clock::AktorGroupClock;
use core::{future::poll_fn, ops::AsyncFnOnce, time::Duration};
#[cfg(feature = "embassy_cross_core")]
use portable_atomic::{AtomicBool, Ordering};
#[cfg(feature = "embassy_cross_core")]
use portable_atomic_util::Arc as Shared;

pub struct AktorGroup<Clock: AktorGroupClock> {
    stop_on_drop: bool,
    spawner: Option<Rc<DriverSpawner>>,
    rejected: Option<RejectedDriver>,
    control: Rc<Control<Clock>>,
    startup_failure: Vec<(crate::ActorId, Rc<Cell<Option<FailureId>>>)>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

type RejectedDriver = Rc<RefCell<Option<LocalFuture<'static, AktorShutdownReport>>>>;

pub struct AktorGroupCompletion<Clock: AktorGroupClock> {
    rejected: Option<RejectedDriver>,
    control: Rc<Control<Clock>>,
}

pub struct AktorKillSwitch<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
    startup_failure: Vec<(crate::ActorId, Rc<Cell<Option<FailureId>>>)>,
}

struct Control<Clock: AktorGroupClock> {
    stopping: Cell<bool>,
    #[cfg(feature = "embassy_cross_core")]
    shared_stopping: Shared<AtomicBool>,
    listening: Cell<bool>,
    driver_loss: Cell<Option<FailureId>>,
    spawner: RefCell<Option<alloc::rc::Weak<DriverSpawner>>>,
    final_hook: RefCell<Option<ShutdownFuture>>,
    shutdown_hook: RefCell<Option<ShutdownHook>>,
    callers: RefCell<Vec<Application>>,
    grace: Duration,
    deadline: Cell<Option<Clock::Deadline>>,
    owners_count: Cell<u32>,
    applications: Cell<u32>,
    published: core::cell::OnceCell<AktorShutdownReport>,
    report: RefCell<crate::Recorder>,
    changed: Event,
}

type DriverSpawner = dyn Fn(LocalFuture<'static, ()>) -> Result<(), String>;

type ShutdownFuture = LocalFuture<'static, Result<(), (AktorCleanupError, String)>>;
type ShutdownHook = Box<dyn FnOnce(AktorShutdownReport) -> ShutdownFuture>;

struct Driver<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

pub(crate) struct OwnerStatus {
    pub cleanup_completed: Option<bool>,
    pub ran_past_grace: bool,
    pub init: crate::AktorInitEvidence,
}

struct Application {
    after: usize,
    policy: crate::AktorJob,
    future: LocalFuture<'static, Result<(), String>>,
}

struct Entry {
    id: crate::ActorId,
    status: Box<dyn Fn() -> OwnerStatus>,
    name: String,
    kind: crate::AktorExecution,
    shutdown: Box<dyn Fn()>,
    /// Drops the running calls once the actor's grace is up, `end` still runs.
    cancel: Option<Box<dyn Fn()>>,
    owner: LocalFuture<'static, Option<String>>,
    finished: bool,
}
