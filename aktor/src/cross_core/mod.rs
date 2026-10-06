use crate::{
    AktorError,
    local::hooks::AktorHooks,
    message::{CallError, LocalFuture},
    operation::Operation,
};
use alloc::{boxed::Box, collections::VecDeque, rc::Rc, vec::Vec};
use core::{cell::RefCell, marker::PhantomData};
use embassy_sync::blocking_mutex::{Mutex, raw::CriticalSectionRawMutex};
use portable_atomic::{AtomicBool, Ordering};
use portable_atomic_util::{Arc, Weak};

mod dispatch;
mod event;
mod impls;
mod latest;
mod request;
use event::{Event, Listener};
pub use impls::channel;
pub use latest::{LatestResults, LatestSender};

type Lock<T> = Mutex<CriticalSectionRawMutex, RefCell<T>>;

/// Callers can transfer this handle. State and operation futures stay on the owner.
pub struct Handle<S, Role = ()> {
    inner: Arc<Inner<S>>,
    role: PhantomData<fn() -> Role>,
}

pub struct WeakHandle<S, Role = ()> {
    inner: Weak<Inner<S>>,
    role: PhantomData<fn() -> Role>,
}

pub struct Owner<S> {
    pub hooks: AktorHooks<S>,
    inner: Arc<Inner<S>>,
    supervisor: Option<(alloc::string::String, crate::embassy::KillSwitch)>,
    local: PhantomData<Rc<()>>,
    intervals: Vec<Interval>,
    interval_cursor: usize,
    work_cursor: usize,
}

#[derive(Clone)]
pub struct Completion {
    status: Arc<Status>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct Request<'a, S, I, O, Role = ()> {
    handle: &'a Handle<S, Role>,
    operation: Operation,
    pending: Option<request::Unsent<S, I, O>>,
    reply: Reply<O>,
    changed: Listener,
    submitted: bool,
    parked: bool,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    answer: Arc<Answer<O>>,
    status: Arc<Status>,
    changed: Listener,
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
    group_stopping: Option<Arc<AtomicBool>>,
    closing: bool,
    finishing: bool,
    ready: Option<Result<(), AktorError>>,
    completed: Option<Result<(), AktorError>>,
    diagnostics: Vec<AktorError>,
}

struct Message<S> {
    operation: Operation,
    job: Box<dyn Job<S>>,
}

trait Job<S>: Send {
    fn run<'a>(
        &'a mut self,
        state: &'a mut S,
        hooks: &'a mut AktorHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'a, ()>;
}

struct Answer<O> {
    result: Lock<AnswerState<O>>,
    changed: Arc<Event>,
}

enum AnswerState<O> {
    Waiting,
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
