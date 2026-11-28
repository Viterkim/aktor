mod impls;

pub use impls::{RequestErr, RetryOptionsErr};

use crate::{AktorDeliveryOutcome, Cause, client::Inner};
use aktor::{operation::Operation, worker::Codec};
use er::*;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{oneshot, watch};

#[derive(Clone, Copy, Debug)]
pub struct AktorRetryOptions {
    /// Total attempts, including the first one.
    pub attempts: core::num::NonZeroUsize,
    /// Budget for each attempt, including connection and queueing.
    pub timeout: Duration,
}

type Answer<O> = Pin<Box<dyn Future<Output = ErResult<O, RequestErr>> + Send>>;

struct Execution<O> {
    answer: Answer<O>,
    admitted: oneshot::Receiver<()>,
    receipt: crate::receipt::Receipt,
}

#[must_use = "await the request or explicitly send it"]
pub struct Request<I, O, C> {
    inner: Arc<Inner>,
    operation: Operation,
    input: Option<Box<I>>,
    options: watch::Sender<AktorRetryOptions>,
    running: Option<Execution<O>>,
    codec: PhantomData<fn() -> C>,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    receipt: crate::receipt::Receipt,
    answer: Option<tokio::task::JoinHandle<ErResult<O, RequestErr>>>,
}
