use super::*;
pub use crate::{ActorArgs, ShutdownReport};
use crate::{ActorFailure, ActorOutcome, AktorError};
use alloc::{string::String, vec::Vec};
use core::{future::poll_fn, ops::AsyncFnOnce, time::Duration};

mod driver;
mod impls;
mod run;
mod shutdown;

pub struct AktorGroup {
    stop_on_drop: bool,
    control: Rc<Control>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

pub struct GroupCompletion {
    control: Rc<Control>,
}

#[derive(Clone)]
pub struct KillSwitch {
    control: Rc<Control>,
}

struct Control {
    stopping: Cell<bool>,
    listening: Cell<bool>,
    grace: Duration,
    deadline: Cell<Option<embassy_time::Instant>>,
    failure: RefCell<Option<ActorFailure>>,
    completed: RefCell<Option<ShutdownReport>>,
    report: RefCell<ShutdownReport>,
    changed: Event,
}

struct Driver {
    control: Rc<Control>,
    owners: Rc<RefCell<Vec<Entry>>>,
}

struct Entry {
    name: String,
    shutdown: Box<dyn Fn()>,
    owner: LocalFuture<'static, Option<AktorCleanupError>>,
    finished: bool,
}
