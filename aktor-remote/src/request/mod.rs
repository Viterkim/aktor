use crate::{CallError, Cause, Error, client::Inner};
use aktor::{operation::Operation, worker::Codec};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{oneshot, watch};

mod impls;

#[derive(Clone, Copy, Debug)]
pub struct AktorRetryOpts {
    /// Total attempts, including the first one.
    pub attempts: usize,
    /// Budget for each attempt, including connection and queueing.
    pub timeout: Duration,
}

type Answer<O> = Pin<Box<dyn Future<Output = Result<O, Error>> + Send>>;

struct Execution<O> {
    answer: Answer<O>,
    admitted: oneshot::Receiver<()>,
}

#[must_use = "await the request or explicitly send it"]
pub struct Request<I, O, C> {
    inner: Arc<Inner>,
    operation: Operation,
    input: Option<Box<I>>,
    options: watch::Sender<AktorRetryOpts>,
    running: Option<Execution<O>>,
    codec: PhantomData<fn() -> C>,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    answer: tokio::task::JoinHandle<Result<O, Error>>,
}
