mod driver;
mod impls;

pub use driver::LatestErr;

use crate::{
    AktorDeliveryOutcome, AktorRetryOptions, Cause,
    client::{Active, Client, Inner},
    wire::{self, Message},
};
use aktor::{
    data::DataError,
    latest::{SendLatest, TypedSession},
    operation::Operation,
    worker::Codec,
};
use er::*;
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

type CodecResult<T> = Result<T, (DataError, String)>;

pub struct LatestSender<I> {
    input: Arc<dyn Input<I> + Send + Sync>,
}

pub struct LatestResults<I, O> {
    receipt: crate::receipt::Receipt,
    shared: Arc<Shared<I, O>>,
    driver: Option<tokio::task::JoinHandle<()>>,
}

trait Input<I> {
    fn send(&self, input: I);
    fn configure(&self, options: AktorRetryOptions);
    fn clone_sender(&self);
    fn close_sender(&self);
}

struct Inputs<I, O> {
    shared: Arc<Shared<I, O>>,
}

struct Shared<I, O> {
    receipt: std::sync::Weak<crate::receipt::Pending>,
    slot: Mutex<Slot<I, O>>,
    changed: Notify,
    cancel: Notify,
    seen: watch::Sender<u64>,
}

struct Slot<I, O> {
    revision: u64,
    pending: Option<Pending<I>>,
    output: Option<ErResult<O, LatestErr>>,
    terminal: Option<ErTree<LatestErr>>,
    senders: usize,
    receiver: bool,
    finished: bool,
    wake: Option<Waker>,
    options: AktorRetryOptions,
}

struct Pending<I> {
    input: Option<I>,
    update: Update,
}

#[derive(Clone)]
struct Update {
    revision: u64,
    // Retries reuse the encoded input after the original value has been dropped.
    input: Option<CodecResult<Arc<Vec<u8>>>>,
    deadline: Instant,
    outcome: AktorDeliveryOutcome,
    uncertain: bool,
    attempts: usize,
    options: AktorRetryOptions,
}
