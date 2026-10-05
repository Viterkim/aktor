use super::*;
pub use crate::{ActorArgs, ShutdownReport};
use crate::{ActorFailure, ActorOutcome, AktorError};
use alloc::{string::String, vec::Vec};
use clock::AktorGroupClock;
use core::{future::poll_fn, ops::AsyncFnOnce, time::Duration};
#[cfg(feature = "embassy_cross_core")]
use portable_atomic::{AtomicBool, Ordering};
#[cfg(feature = "embassy_cross_core")]
use portable_atomic_util::Arc as Shared;

mod driver;
mod impls;
mod run;
mod shutdown;

pub struct AktorGroup<Clock: AktorGroupClock> {
    stop_on_drop: bool,
    control: Rc<Control<Clock>>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

pub struct GroupCompletion<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
}

pub struct KillSwitch<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
}

struct Control<Clock: AktorGroupClock> {
    stopping: Cell<bool>,
    #[cfg(feature = "embassy_cross_core")]
    shared_stopping: Shared<AtomicBool>,
    listening: Cell<bool>,
    grace: Duration,
    deadline: Cell<Option<Clock::Deadline>>,
    kinds: RefCell<Vec<(String, crate::AktorExecution)>>,
    failure: RefCell<Option<ActorFailure>>,
    completed: RefCell<Option<ShutdownReport>>,
    report: RefCell<ShutdownReport>,
    changed: Event,
}

struct Driver<Clock: AktorGroupClock> {
    control: Rc<Control<Clock>>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

struct Entry {
    name: String,
    kind: crate::AktorExecution,
    shutdown: Box<dyn Fn()>,
    owner: LocalFuture<'static, Option<AktorCleanupError>>,
    diagnostics: Rc<RefCell<Vec<AktorError>>>,
    finished: bool,
}
