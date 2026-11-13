#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
use crate::threads::lock::Mutex;
use crate::{
    AktorError, AktorFailure,
    local::hooks::CallHooks,
    message::{CallError, LocalFuture},
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
use portable_atomic_util::{Arc, Weak};

mod dispatch;
#[doc(hidden)]
pub mod event;
mod impls;
mod latest;
mod request;
use event::{Event, Listener};
#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
pub(crate) use impls::browser_channel;
pub use impls::channel;
pub use latest::{LatestResults, LatestSender};

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

#[doc(hidden)]
pub struct Owner<S> {
    pub hooks: CallHooks<S>,
    inner: Arc<Inner<S>>,
    #[cfg(feature = "embassy_cross_core")]
    supervisor: Option<(alloc::string::String, crate::embassy::AktorKillSwitch)>,
    local: PhantomData<Rc<()>>,
    intervals: Vec<Interval>,
    interval_cursor: usize,
    work_cursor: usize,
}

#[derive(er::Er)]
#[er(no_constructors)]
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
    parked: bool,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    answer: Arc<Answer<O>>,
    status: Arc<Status>,
    parked: bool,
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

struct Status {
    state: Lock<State>,
    changed: Arc<Event>,
}

#[derive(Default)]
struct State {
    cut_call: bool,
    #[cfg(all(feature = "std", feature = "embassy_cross_core"))]
    panic_reported: bool,
    cleanup_completed: bool,
    managed: bool,
    /// The group lives on one thread or core, it says so here once it starts closing.
    group_stopped: bool,
    closing: bool,
    finishing: bool,
    ready: Option<Result<(), AktorError>>,
    completed: Option<Result<(), AktorError>>,
    diagnostics: Vec<AktorError>,
    cleanup_failure: Option<AktorFailure>,
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
    Ready(Result<O, CallError>),
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
