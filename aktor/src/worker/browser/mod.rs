use super::*;
use crate::{message::LocalFuture, operation::Operation};
use core::{future::Future, marker::PhantomData};
use gloo_timers::future::TimeoutFuture;
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
mod open;
mod server;
use completion::observe;
use impls::fatal;
pub use server::{Server, serve, serve_with, setup_failed};

pub struct Worker<S, Role = ()> {
    inner: Rc<Inner>,
    state: PhantomData<fn() -> (S, Role)>,
}

pub struct Completion {
    result: watch::Receiver<Option<Result<(), WorkerError>>>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct WorkerRequest<'a, S, O, Role = ()> {
    worker: &'a Worker<S, Role>,
    operation: String,
    input: Option<Result<String, WorkerError>>,
    admission:
        Option<LocalFuture<'a, Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), WorkerError>>>,
    reply: Option<WorkerReply<O>>,
    latest: Option<String>,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct WorkerReply<O> {
    response: oneshot::Receiver<Result<String, WorkerError>>,
    inner: Weak<Inner>,
    id: u64,
    timeout: TimeoutFuture,
    output: PhantomData<fn() -> O>,
}

#[must_use = "await the checked reply to receive the operation's result"]
pub struct CheckedWorkerReply<O>(WorkerReply<O>);

type Callback<Event> = RefCell<Option<Closure<dyn FnMut(Event)>>>;

struct Work {
    answer: Option<oneshot::Sender<Result<String, WorkerError>>>,
    input: Option<Incoming>,
    latest: Option<(String, String)>,
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

struct Inner {
    worker: web_sys::Worker,
    outstanding: RefCell<HashMap<u64, Work>>,
    queue: RefCell<VecDeque<u64>>,
    active: Cell<Option<u64>>,
    executing: Cell<bool>,
    shutdown_sent: Cell<bool>,
    next: Cell<u64>,
    handles: Cell<usize>,
    closed: Cell<bool>,
    options: Options,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    ready: watch::Sender<Option<Result<(), WorkerError>>>,
    finished: watch::Sender<Option<Result<(), WorkerError>>>,
    retained: RefCell<Option<Rc<Inner>>>,
    message: Callback<MessageEvent>,
    error: Callback<ErrorEvent>,
}
