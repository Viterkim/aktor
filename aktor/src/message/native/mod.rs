mod call;
mod impls;
mod latest;
#[cfg(test)]
mod tests;

pub use crate::queue::latest::{LatestResults, LatestSender};
#[cfg(not(target_family = "wasm"))]
pub(crate) use call::detached;
pub use call::{call, call_async};
pub use impls::request::{RequestCast, RequestSend};

use super::*;
use crate::queue::mailbox;
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use parking_lot::Mutex;
use std::sync::Arc;

/// Nothing is sent just by constructing it.
#[must_use = "await the request, or explicitly send or cast it"]
pub struct Request<'a, S, O> {
    sender: &'a mailbox::Sender<Message<S>>,
    admission: &'a Arc<crate::queue::Admission>,
    submission: Submission<'a, S>,
    reply: Reply<O>,
}

enum Submission<'a, S> {
    Unsent(Message<S>),
    Waiting {
        message: Message<S>,
        admission: Admission<'a, S>,
    },
    Submitted,
    Closed,
    Consumed,
}

type Admission<'a, S> =
    Pin<Box<dyn Future<Output = Option<mailbox::Permit<'a, Message<S>>>> + Send + 'a>>;

/// A reply to await later. Only handles keep the actor alive.
#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    receive: bool,
    admission: Arc<crate::queue::Admission>,
    answer: Arc<dyn Answer<O>>,
    taken: bool,
}

trait Answer<O>: Send + Sync {
    fn try_take(&self) -> Option<Result<O, crate::CallLoss>>;
    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<O, crate::CallLoss>>;
    fn abandon(&self);
}

pub struct Message<S> {
    pub operation: crate::listener::Operation,
    job: Arc<dyn Job<S>>,
    finished: bool,
}

trait Job<S>: Send + Sync {
    fn service_id(&self) -> Option<*const ()> {
        None
    }

    fn run<'a>(
        &'a self,
        state: &'a mut S,
        hooks: &'a mut crate::listener::hooks::CallHooks<S>,
        operation: crate::operation::Operation,
    ) -> LocalFuture<'a, ()>;
    fn close(&self);
}

struct AsyncJob<F, I, O>(Packet<F, I, O>);

struct Packet<F, I, O> {
    admission: Option<Arc<crate::queue::Admission>>,
    data: Mutex<Data<F, I, O>>,
}

struct Data<F, I, O> {
    function: Option<(F, I)>,
    completion: Completion<O>,
}

enum Completion<O> {
    Waiting(Option<Waker>),
    Ready(Result<O, crate::CallLoss>),
    Abandoned,
    Consumed,
}
