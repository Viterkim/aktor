use super::*;
use crate::{message::LocalFuture, operation::Operation};
use core::{future::Future, marker::PhantomData};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    rc::{Rc, Weak},
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};
use wasm_bindgen::closure::Closure;
use web_sys::{ErrorEvent, MessageEvent};

mod completion;
mod impls;
mod latest;
mod open;
mod server;
mod setup;
use completion::observe;
use impls::fatal;
pub use latest::{LatestResults, LatestSender};
pub use server::{Server, serve, serve_for, serve_with, setup_failed};
pub use setup::serve_setup;

type State<S, Role, E> = PhantomData<fn() -> (S, Role, E)>;
type EncodeInput<'a> = Box<dyn FnOnce() -> Result<Vec<u8>, WireError> + 'a>;

pub struct Worker<S, Role = (), E = ()> {
    inner: Rc<Inner>,
    state: State<S, Role, E>,
}

pub struct Completion<E = ()> {
    data: PhantomData<fn() -> E>,
    result: watch::Receiver<Option<Result<(), WireError>>>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct WorkerRequest<'a, S, O, Role = ()> {
    inner: &'a Rc<Inner>,
    state: PhantomData<fn() -> (S, Role)>,
    operation: String,
    input: Option<Result<Vec<u8>, WireError>>,
    encoder: Option<EncodeInput<'a>>,
    admission:
        Option<LocalFuture<'a, Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), WireError>>>,
    reply: Option<WorkerReply<O>>,
    parked: bool,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct WorkerReply<O> {
    response: oneshot::Receiver<Result<Vec<u8>, WireError>>,
    inner: Weak<Inner>,
    group: Option<(String, crate::group::KillSwitch)>,
    id: u64,
    parked: bool,
    taken: bool,
    output: PhantomData<fn() -> O>,
}

type Callback<Event> = RefCell<Option<Closure<dyn FnMut(Event)>>>;

struct Work {
    answer: Option<oneshot::Sender<Result<Vec<u8>, WireError>>>,
    input: Option<Incoming>,
    service: Option<Box<dyn latest::Service>>,
    _count: Option<OwnedSemaphorePermit>,
    _bytes: Option<OwnedSemaphorePermit>,
}

struct Inner {
    worker: web_sys::Worker,
    outstanding: RefCell<HashMap<u64, Work>>,
    queue: RefCell<VecDeque<u64>>,
    active: Cell<Option<u64>>,
    executing: Cell<bool>,
    pump_scheduled: Cell<bool>,
    shutdown_sent: Cell<bool>,
    next: Cell<u64>,
    handles: Cell<usize>,
    closed: Cell<bool>,
    failed: Cell<bool>,
    options: Options,
    initialize: RefCell<Option<Vec<u8>>>,
    group: RefCell<Option<(String, crate::group::KillSwitch)>>,
    sessions: RefCell<Vec<Weak<dyn Fn() -> bool>>>,
    prune_at: Cell<usize>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    ready: watch::Sender<Option<Result<(), WireError>>>,
    finished: watch::Sender<Option<Result<(), WireError>>>,
    retained: RefCell<Option<Rc<Inner>>>,
    message: Callback<MessageEvent>,
    error: Callback<ErrorEvent>,
}
