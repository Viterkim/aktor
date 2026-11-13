use super::*;
use crate::{AktorRetireStatus, message::LocalFuture, operation::Operation};
use core::{future::Future, marker::PhantomData};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    rc::{Rc, Weak},
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};
use wasm_bindgen::closure::Closure;
use web_sys::{Event, MessageEvent};

mod buffer;
mod completion;
mod dependency;
pub use dependency::join;
mod impls;
mod latest;
mod open;
mod optional;
mod server;
mod setup;
use buffer::Buffers;
use completion::observe;
use impls::fatal;
pub use latest::{LatestResults, LatestSender};
pub use optional::{OptionalReply, OptionalRequest, OptionalWorker};
pub use server::{
    Server, report_cleanup_pending, report_setup_pending, retain_cleanup, retain_setup, serve,
    serve_for, serve_with, setup_failed,
};
pub use setup::{AktorServe, serve_setup};

type State<S, Role, E> = PhantomData<fn() -> (S, Role, E)>;
type Answer = Box<dyn FnOnce(Result<Vec<u8>, WireError>) -> Option<WireError>>;
type EncodeInput<'a> = Box<dyn FnOnce() -> Result<Vec<u8>, WireError> + 'a>;

/// Why a worker never became ready. Match `init` to handle the initializer's own error.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, er::Er)]
#[er(no_constructors, format = Self::fmt_display)]
#[non_exhaustive]
pub struct WorkerStartError<E = ()> {
    pub init: crate::AktorInitOutcome<E>,
    pub outcome: CallError,
    pub cause: WorkerCause,
}

/// The result of the worker's own `end` callback.
#[derive(Clone, PartialEq, Eq, er::Er)]
#[er(no_constructors)]
pub enum WorkerCleanupError<E = ()> {
    /// `end` returned an error. A worker that sends typed error data puts it here.
    #[er(format = "worker cleanup failed")]
    Failed(Option<E>),
    /// The page never received a result from `end`.
    #[er(format = "worker cleanup result is unconfirmed")]
    Unconfirmed,
}

#[derive(Clone)]
pub(crate) struct ReadyFailure<T = Vec<u8>> {
    error: WorkerFailure<T>,
    init: crate::AktorInitOutcome<core::convert::Infallible>,
}

pub struct Worker<S, Role = (), E = ()> {
    inner: Rc<Inner>,
    state: State<S, Role, E>,
}

/// Observe this worker after dropping its handle.
pub struct Completion<E = ()> {
    data: PhantomData<fn() -> E>,
    result: watch::Receiver<Option<Result<(), WireError>>>,
    diagnostics: Rc<RefCell<Vec<crate::AktorError>>>,
    retirement: watch::Receiver<AktorRetireStatus>,
    end: watch::Receiver<End>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct WorkerRequest<'a, S, O, Role = ()> {
    inner: &'a Rc<Inner>,
    state: PhantomData<fn() -> (S, Role)>,
    operation: String,
    input: Option<Result<Vec<u8>, WireError>>,
    encoder: Option<EncodeInput<'a>>,
    decoder: fn(&[u8]) -> Result<O, WorkerError>,
    admission:
        Option<LocalFuture<'a, Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), WireError>>>,
    reply: Option<WorkerReply<O>>,
    parked: bool,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct WorkerReply<O> {
    response: oneshot::Receiver<Result<O, WireError>>,
    inner: Weak<Inner>,
    group: Option<(String, crate::group::AktorKillSwitch)>,
    id: u64,
    parked: bool,
    taken: bool,
    error: Option<WireError>,
}

type Callback<Event> = RefCell<Option<Closure<dyn FnMut(Event)>>>;

struct Work {
    answer: Option<Answer>,
    input: Option<Incoming>,
    service: Option<Box<dyn latest::Service>>,
    _count: Option<OwnedSemaphorePermit>,
    _bytes: Option<OwnedSemaphorePermit>,
}

/// Owns the external work supplied to [`join`] until shutdown or a cancelled setup joins it.
pub struct WorkerJoin {
    joining: Option<LocalFuture<'static, Result<(), crate::AktorError>>>,
    receipt: watch::Receiver<Option<Result<(), crate::AktorError>>>,
}

enum End {
    Waiting,
    Returned(Result<(), WireError>),
    Unconfirmed(Option<String>),
}

enum ShutdownJoin {
    Ready,
    Waiting(LocalFuture<'static, Result<(), crate::AktorError>>),
    Joining { force: bool },
}

struct Inner {
    worker: web_sys::Worker,
    outstanding: RefCell<HashMap<u64, Work>>,
    queue: RefCell<VecDeque<u64>>,
    active: Cell<Option<u64>>,
    executing: Cell<bool>,
    pump_scheduled: Cell<bool>,
    shutdown_sent: Cell<bool>,
    shutdown_join: RefCell<ShutdownJoin>,
    pending_finish: RefCell<Option<Result<(), WireError>>>,
    next: Cell<u64>,
    handles: Cell<usize>,
    closed: Cell<bool>,
    failed: Cell<bool>,
    waiting_to_terminate: Cell<bool>,
    optional: bool,
    accepted: Cell<bool>,
    retain_unconfirmed: Cell<bool>,
    uncertain: RefCell<Vec<Work>>,
    failure: RefCell<Option<WireError>>,
    setup_failure_pending: Cell<bool>,
    diagnostics: Rc<RefCell<Vec<crate::AktorError>>>,
    options: Options,
    buffers: Buffers,
    initialize: RefCell<Option<Vec<u8>>>,
    group: RefCell<Option<(String, crate::group::AktorKillSwitch)>>,
    sessions: RefCell<Vec<Weak<dyn Fn() -> bool>>>,
    prune_at: Cell<usize>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    ready: watch::Sender<Option<Result<(), ReadyFailure>>>,
    finished: watch::Sender<Option<Result<(), WireError>>>,
    retirement: watch::Sender<AktorRetireStatus>,
    end: watch::Sender<End>,
    retained: RefCell<Option<Rc<Inner>>>,
    message: Callback<MessageEvent>,
    error: Callback<Event>,
}
