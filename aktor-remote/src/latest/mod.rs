use crate::{
    AktorRetryOpts, CallError, Cause, Error,
    client::{Active, Client, Inner},
    wire::{self, Message},
};
use aktor::{
    latest::{SendLatest, TypedSession},
    operation::Operation,
    worker::Codec,
};
use std::{
    future::{Future, poll_fn},
    ops::AsyncFnOnce,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::{
    sync::{Notify, watch},
    time::{Instant, sleep_until, timeout_at},
};

mod driver;
mod impls;

pub struct LatestSender<I> {
    input: Arc<dyn Input<I> + Send + Sync>,
}

pub struct LatestResults<I, O> {
    shared: Arc<Shared<I, O>>,
    driver: Option<tokio::task::JoinHandle<()>>,
}

trait Input<I> {
    fn send(&self, input: I);
    fn configure(&self, options: AktorRetryOpts);
    fn clone_sender(&self);
    fn close_sender(&self);
}

struct Inputs<I, O> {
    shared: Arc<Shared<I, O>>,
}

struct Shared<I, O> {
    slot: Mutex<Slot<I, O>>,
    changed: Notify,
    cancel: Notify,
    seen: watch::Sender<u64>,
}

struct Slot<I, O> {
    revision: u64,
    pending: Option<Pending<I>>,
    output: Option<Result<O, Error>>,
    senders: usize,
    receiver: bool,
    finished: bool,
    wake: Option<Waker>,
    options: AktorRetryOpts,
}

struct Pending<I> {
    input: Option<I>,
    update: Update,
}

#[derive(Clone)]
struct Update {
    revision: u64,
    input: Option<Result<Arc<[u8]>, Error>>,
    deadline: Instant,
    outcome: CallError,
    uncertain: bool,
    attempts: usize,
    options: AktorRetryOpts,
}
