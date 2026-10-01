use super::*;
use crate::queue::{key::LatestKey, mailbox};
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

mod call;
mod impls;
#[cfg(test)]
mod tests;

pub use call::{call, call_async};

/// Nothing is sent just by constructing it.
#[must_use = "await the request, or explicitly send or cast it"]
pub struct Request<'a, S, O> {
    sender: &'a mailbox::Sender<S>,
    admission: &'a Arc<crate::queue::Admission>,
    submission: Submission<'a, S>,
    reply: Reply<O>,
}

#[must_use = "await the checked request to submit it and receive its result"]
pub struct CheckedRequest<'a, S, O>(Request<'a, S, O>);

enum Submission<'a, S> {
    Unsent(Message<S>),
    Waiting {
        message: Message<S>,
        admission: Admission<'a, S>,
        epoch: u64,
    },
    Submitted,
    Closed,
    Consumed,
}

type Admission<'a, S> =
    Pin<Box<dyn Future<Output = Result<mailbox::Permit<'a, S>, ()>> + Send + 'a>>;

/// A reply to await later. Only handles keep the actor alive.
#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    answer: Arc<dyn Answer<O>>,
    finished: watch::Receiver<()>,
    closing: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    error: Option<CallError>,
}

#[must_use = "await the checked reply to receive the operation's result"]
pub struct CheckedReply<O>(Reply<O>);

trait Answer<O>: Send + Sync {
    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<O, CallError>>;
    fn abandon(&self);
}

pub struct Message<S> {
    pub operation: crate::listener::Operation,
    job: Arc<dyn Job<S>>,
    finished: bool,
    latest: Option<LatestKey>,
}

trait Job<S>: Send + Sync {
    fn run<'a>(&'a self, state: &'a mut S) -> LocalFuture<'a, ()>;
    fn close(&self);
    fn supersede(&self);
}

struct AsyncJob<F, I, O>(Arc<Packet<F, I, O>>);

struct Packet<F, I, O> {
    data: Mutex<Data<F, I, O>>,
}

struct Data<F, I, O> {
    function: Option<(F, I)>,
    completion: Completion<O>,
}

enum Completion<O> {
    Waiting(Option<Waker>),
    Ready(Result<O, CallError>),
    Abandoned,
    Consumed,
}
