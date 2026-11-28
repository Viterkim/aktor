mod buffer;
mod completion;
mod dependency;
mod impls;
mod latest;
mod open;
mod optional;
#[cfg(feature = "wasm_browser_worker_server")]
mod server;
#[cfg(feature = "wasm_browser_worker_server")]
mod setup;

pub use dependency::join;
pub use impls::{WorkerRequestCast, WorkerRequestSend};
pub use latest::{LatestResults, LatestSender};
pub use optional::{OptionalRequest, OptionalRequestSend, OptionalWorker};
#[cfg(feature = "wasm_browser_worker_server")]
pub use setup::{AktorReady, AktorServe, ServeFailure, serve_setup};

use super::*;
use crate::{
    AktorRetirement, AktorWorkerContract,
    data::{AktorData, decode, encode},
    message::LocalFuture,
    operation::Operation,
};
use buffer::Buffers;
use completion::observe;
use core::{future::Future, marker::PhantomData};
use er::*;
#[cfg(feature = "wasm_browser_worker_server")]
use impls::fatal;
use std::{
    cell::{Cell, OnceCell, RefCell},
    collections::{HashMap, VecDeque},
    rc::{Rc, Weak},
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};
use wasm_bindgen::closure::Closure;
use web_sys::{Event, MessageEvent};

type Encoder<T> = fn(&T) -> Result<Vec<u8>, (crate::data::DataError, String)>;
type Decoder<T> = fn(&[u8]) -> Result<T, (WorkerError, String)>;

fn decode_output<C: Codec<T>, T>(bytes: &[u8]) -> Result<T, (WorkerError, String)> {
    Ok(crate::worker::wire::codec(C::decode(bytes), || {
        WorkerError::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Codec)
    })?)
}

type Answer = Box<
    dyn FnOnce(
        Result<Vec<u8>, WireFailure>,
        &dyn Fn(WireFailure) -> WireFailure,
    ) -> Option<WireFailure>,
>;
type EncodeInput<'a> = Box<dyn FnOnce() -> Result<Vec<u8>, WireFailure> + 'a>;

/// A worker that never entered its initializer can still report why it could not open.
#[derive(Clone, PartialEq, Eq, Er)]
pub enum WorkerStartError<E = ()> {
    #[er(format = "{0}")]
    Refused(WorkerStartFailure),
    #[er(format = "{failure}")]
    Initializer {
        #[er(skip)]
        error: E,
        failure: WorkerStartFailure,
    },
    #[er(format = "{0}")]
    Failed(WorkerStartFailure),
}

#[derive(Clone, PartialEq, Eq, Er)]
#[er(format = "worker call {outcome:?}: {cause}")]
pub struct WorkerStartFailure {
    pub reason: crate::AktorStartReason,
    pub failure_id: Option<crate::FailureId>,
    pub evidence: crate::AktorInitEvidence,
    pub outcome: AktorDeliveryOutcome,
    pub cause: WorkerCause,
}

/// The result of the worker's own `end` callback.
#[derive(Clone, PartialEq, Eq, Er)]
pub enum WorkerCleanupError<E = ()> {
    /// `end` returned an error. Its report goes into the group's shutdown report, and a worker
    /// that sends typed error data puts it here.
    #[er(format = "worker cleanup failed")]
    Failed(Option<E>, #[er(skip)] Option<crate::FailureId>),
    /// The page never received a result from `end`.
    #[er(format = "worker cleanup result is unconfirmed")]
    Unconfirmed(#[er(skip)] Option<crate::FailureId>),
}

#[derive(Clone)]
pub(crate) struct ReadyFailure {
    reason: crate::AktorStartReason,
    pub(crate) error: WireFailure,
    evidence: crate::AktorInitEvidence,
}

type WorkerReceipt<P, E> = Result<P, (WorkerStartError<E>, String)>;

/// The page handle for a required worker. Its contract determines which calls it accepts.
pub type Worker<C> = WorkerHandle<C, <C as AktorWorkerContract>::State>;

#[doc(hidden)]
pub struct WorkerHandle<C: AktorWorkerContract, S> {
    state: PhantomData<fn() -> S>,
    receipt: Rc<OnceCell<WorkerReceipt<C::Ready, C::Error>>>,
    inner: Rc<Inner>,
}

/// Observe this worker after dropping its handle.
pub struct Completion<E = ()> {
    data: PhantomData<fn() -> E>,
    result: watch::Receiver<Option<Result<(), WireFailure>>>,
    group: crate::AktorKillSwitch,
    retirement: watch::Receiver<Option<AktorRetirement>>,
    end: watch::Receiver<End>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct WorkerRequest<'a, S, O, Role = ()> {
    inner: &'a Rc<Inner>,
    state: PhantomData<fn() -> (S, Role)>,
    operation: Operation,
    input: Option<Vec<u8>>,
    encoder: Option<EncodeInput<'a>>,
    decoder: Decoder<O>,
    admission:
        Option<LocalFuture<'a, Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), WireFailure>>>,
    reply: Option<WorkerReply<O>>,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct WorkerReply<O> {
    response: oneshot::Receiver<Result<O, WireFailure>>,
    inner: Weak<Inner>,
    group: crate::AktorKillSwitch,
    id: u64,
    taken: bool,
}

type Callback<Event> = RefCell<Option<Closure<dyn FnMut(Event)>>>;

struct Work {
    operation: Operation,
    answer: Option<Answer>,
    input: Option<Incoming>,
    service: Option<Box<dyn latest::Service>>,
    _count: Option<OwnedSemaphorePermit>,
    _bytes: Option<OwnedSemaphorePermit>,
}

/// Owns the external work supplied to [`join`] until shutdown or a cancelled setup joins it.
pub struct WorkerJoin {
    joining: Option<LocalFuture<'static, WorkerJoinOutcome>>,
    receipt: JoinReceipt,
}

pub type WorkerRetired<E> =
    Result<crate::AktorRetired<Result<(), (WorkerError<E>, String)>>, (WorkerError<E>, String)>;

type JoinReceipt = watch::Receiver<Option<WorkerJoinOutcome>>;

/// Whether the work that owns this worker has stopped.
#[derive(Clone)]
pub enum WorkerJoinOutcome {
    Stopped(Result<(), (crate::AktorCleanupError, String)>),
    Unconfirmed(String),
}

/// The receipt from the worker's own end callback.
#[derive(Debug)]
#[must_use]
pub enum AktorCleanup<E> {
    Pending,
    Returned(Result<(), WorkerFailureReceipt<WorkerEndError<E>>>),
    Unconfirmed(WorkerFailureReceipt<String>),
}

#[derive(Debug)]
pub struct WorkerEndError<E> {
    pub data: Option<E>,
    pub failure_id: Option<crate::FailureId>,
    pub report: String,
}

/// Inspect the outcome freely. Taking its error tells the group it was received.
#[derive(Debug)]
pub struct WorkerFailureReceipt<T> {
    error: T,
    delivery: Option<crate::Delivery>,
}
impl<T> WorkerFailureReceipt<T> {
    pub fn failure_id(&self) -> Option<crate::FailureId> {
        self.delivery.as_ref().map(|delivery| delivery.0)
    }

    pub fn take(self) -> T {
        if let Some(delivery) = self.delivery {
            delivery.receive();
        }
        self.error
    }
}

enum End {
    Waiting,
    Returned(Result<(), WireFailure>),
    Unconfirmed(WireFailure),
}

enum ShutdownJoin {
    Ready,
    Waiting(LocalFuture<'static, WorkerJoinOutcome>),
    Unconfirmed,
    Joining { force: bool },
}

fn deliver(id: Option<crate::FailureId>, group: &crate::AktorKillSwitch) {
    if let Some(delivery) = id.and_then(|id| group.delivery(id)) {
        delivery.receive();
    }
}

struct Inner {
    /// `None` for a worker that never launched, every call on it fails.
    worker: Option<web_sys::Worker>,
    outstanding: RefCell<HashMap<u64, Work>>,
    queue: RefCell<VecDeque<u64>>,
    active: Cell<Option<u64>>,
    executing: Cell<bool>,
    terminate_requested: Cell<bool>,
    pump_scheduled: Cell<bool>,
    shutdown_sent: Cell<bool>,
    shutdown_join: RefCell<ShutdownJoin>,
    pending_finish: RefCell<Option<Result<(), WireFailure>>>,
    next: Cell<u64>,
    handles: Cell<usize>,
    closed: Cell<bool>,
    failed: Cell<bool>,
    optional: bool,
    accepted: Cell<bool>,
    retain_unconfirmed: Cell<bool>,
    uncertain: RefCell<Vec<Work>>,
    failure: RefCell<Option<WireFailure>>,
    cleanup_pending: Cell<Option<crate::FailureId>>,
    init_entered: Cell<bool>,
    setup_failure_pending: Cell<bool>,
    options: Options,
    buffers: Buffers,
    initialize: RefCell<Option<(&'static str, Vec<u8>)>>,
    name: String,
    group: crate::AktorKillSwitch,
    sessions: RefCell<Vec<Weak<dyn Fn() -> bool>>>,
    prune_at: Cell<usize>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    ready: watch::Sender<Option<Result<(), ReadyFailure>>>,
    finished: watch::Sender<Option<Result<(), WireFailure>>>,
    retirement: watch::Sender<Option<AktorRetirement>>,
    end: watch::Sender<End>,
    retained: RefCell<Option<Rc<Inner>>>,
    message: Callback<MessageEvent>,
    error: Callback<Event>,
}

#[derive(Er)]
#[er(format = "{message}")]
struct BrowserErr {
    message: String,
}

#[wasm_bindgen::prelude::wasm_bindgen(
    inline_js = "export function browserException(error) { return String(error?.stack || error); }"
)]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(js_name = browserException)]
    fn browser_exception(error: wasm_bindgen::JsValue) -> String;
}
fn browser_result<T>(result: Result<T, wasm_bindgen::JsValue>) -> ErResult<T, BrowserErr> {
    match result {
        Ok(value) => Ok(value),
        Err(exception) => er_bail!(BrowserErr::new(browser_exception(exception))),
    }
}

#[derive(Er)]
enum ReadFrameErr {
    ExpectedBuffer,
    Decode,
}
fn read_frame<T: AktorData>(
    event: MessageEvent,
) -> ErResult<(T, js_sys::ArrayBuffer), ReadFrameErr> {
    use wasm_bindgen::JsCast;

    let buffer = event
        .data()
        .dyn_into::<js_sys::ArrayBuffer>()
        .ok()
        .er(|| ReadFrameErr::ExpectedBuffer)?;
    let frame = decode(&js_sys::Uint8Array::new(&buffer).to_vec()).er(|| ReadFrameErr::Decode)?;

    Ok((frame, buffer))
}
impl ReadFrameErr {
    fn cause(&self) -> WorkerCause {
        match self {
            Self::ExpectedBuffer => WorkerCause::Protocol,
            Self::Decode => WorkerCause::Codec,
        }
    }
}

impl From<WireFailure> for crate::AktorDeliveryError {
    fn from(error: WireFailure) -> Self {
        Self {
            outcome: error.error.outcome,
            report: error.report,
            failure_id: error.error.failure_id,
        }
    }
}
