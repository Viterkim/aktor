#[doc(hidden)]
pub mod event;

mod dispatch;
mod impls;
mod latest;
mod request;

pub use impls::Run;
pub(crate) use impls::RunErr;
#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
pub(crate) use impls::browser_channel;
pub use impls::channel;
pub(crate) use impls::closed_channel;
pub use latest::{LatestResults, LatestSender};
pub use request::{RequestCast, RequestSend};

#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
use crate::threads::lock::Mutex;
use crate::{
    local::hooks::CallHooks,
    message::{AktorDeliveryOutcome, LocalFuture},
    operation::Operation,
};
use alloc::{boxed::Box, collections::VecDeque, rc::Rc, string::String, vec::Vec};
use core::{cell::RefCell, marker::PhantomData, task::Waker};
#[cfg(not(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
)))]
use embassy_sync::blocking_mutex::{Mutex, raw::CriticalSectionRawMutex};
use er::*;
use event::{Event, Listener};
use portable_atomic_util::{Arc, Weak};

#[cfg(not(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
)))]
type Lock<T> = Mutex<CriticalSectionRawMutex, RefCell<T>>;
#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
type Lock<T> = Mutex<RefCell<T>>;

/// Callers can transfer this handle. State and operation futures stay on the owner.
pub struct Handle<S, Role = ()> {
    inner: Arc<Inner<S>>,
    role: PhantomData<fn() -> Role>,
}

pub struct WeakHandle<S, Role = ()> {
    inner: Weak<Inner<S>>,
    role: PhantomData<fn() -> Role>,
}

pub(crate) struct Control<S> {
    inner: Arc<Inner<S>>,
}
impl<S> Clone for Control<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<S> Control<S> {
    pub(crate) fn shutdown(&self) {
        self.inner.close();
    }

    pub(crate) fn cancel_calls(&self) {
        self.inner.cancel_calls();
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_os = "unknown",
        target_feature = "atomics"
    ))]
    pub(crate) fn completion(&self) -> Completion {
        Completion {
            status: self.inner.status.clone(),
        }
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_os = "unknown",
        target_feature = "atomics"
    ))]
    pub(crate) fn finish_failed(&self, error: String, delivery: crate::Delivery) {
        if self
            .inner
            .status
            .state
            .lock(|state| state.borrow().completed.is_none())
        {
            self.inner.finish(Err(error), Some(delivery));
        }
    }
}

#[doc(hidden)]
pub struct Owner<S> {
    pub hooks: CallHooks<S>,
    inner: Arc<Inner<S>>,
    supervisor: Option<Supervisor>,
    local: PhantomData<Rc<()>>,
    intervals: Vec<Interval>,
    interval_cursor: usize,
    work_cursor: usize,
}

#[derive(Clone, Copy, Er)]

pub enum OwnerError {
    #[er(format = "actor setup failed")]
    Setup,
    #[er(format = "actor owner failed")]
    Completion,
}

#[derive(Clone)]
#[doc(hidden)]
pub struct Completion {
    status: Arc<Status>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct Request<'a, S, I, O, Role = ()> {
    handle: &'a Handle<S, Role>,
    operation: Operation,
    pending: Option<request::Unsent<S, I, O>>,
    reply: Reply<O>,
    changed: Option<Listener>,
    submitted: bool,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    closed: Option<Listener>,
    answer: Arc<Answer<O>>,
    status: Arc<Status>,
    taken: bool,
}

struct Inner<S> {
    queue: Lock<Queue<S>>,
    capacity: usize,
    changed: Arc<Event>,
    status: Arc<Status>,
}

struct Queue<S> {
    open: bool,
    /// Set when shutdown cuts the running call, the owner stops serving and runs `end`.
    cancelled: bool,
    handles: usize,
    ordinary: VecDeque<Message<S>>,
    services: VecDeque<Message<S>>,
}

#[derive(Clone)]
struct Supervisor {
    name: String,
    kind: crate::AktorExecution,
    kill: crate::local::StopSignal,
}

struct Status {
    state: Lock<State>,
    changed: Arc<Event>,
}

struct State {
    actor_id: Option<crate::ActorId>,
    returned: Vec<crate::Delivery>,
    lost: Option<crate::Delivery>,
    refused: Option<crate::Delivery>,
    cut_call: bool,
    /// `end` ran out of its grace and got dropped.
    ran_out: bool,
    init: crate::AktorInitEvidence,
    cleanup_completed: bool,
    managed: bool,
    /// The group lives on one thread or core, it says so here once it starts closing.
    group_stopped: bool,
    closing: bool,
    finishing: bool,
    ready: Option<Result<(), String>>,
    completed: Option<Result<(), String>>,
    diagnostics: Vec<String>,
    #[cfg(feature = "std")]
    pending_drops: Vec<String>,
    #[cfg(feature = "std")]
    phase: Option<Activity>,
    cleanup_failure: Option<(RunErr, String)>,
}

#[cfg(feature = "std")]
#[derive(Clone)]
enum Activity {
    Phase(crate::AktorPhase),
    Call(Operation),
}

struct Message<S> {
    operation: Operation,
    job: Box<dyn Job<S>>,
}

enum Refused<S> {
    Full(Message<S>),
    Closed(Message<S>),
}

trait Job<S>: Send {
    fn service_id(&self) -> Option<*const ()> {
        None
    }

    fn run<'a>(
        &'a mut self,
        state: &'a mut S,
        hooks: &'a mut CallHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'a, ()>;
}

struct Answer<O> {
    result: Lock<AnswerState<O>>,
}

enum AnswerState<O> {
    Waiting(Option<Waker>),
    Ready(Result<O, crate::CallLoss>),
    Abandoned,
    Consumed,
}

struct Interval {
    every: core::time::Duration,
    callback: usize,
    ready: bool,
    timer: Option<LocalFuture<'static, ()>>,
}

enum Work<S> {
    Message(Message<S>),
    Interval(usize),
}
