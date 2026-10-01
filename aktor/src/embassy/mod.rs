use crate::{
    message::{ActorError, CallError, LocalFuture},
    operation::Operation,
};
use alloc::{
    boxed::Box,
    rc::{Rc, Weak},
};
use core::{
    cell::{Cell, RefCell},
    future::Future,
    marker::PhantomData,
    task::{Poll, Waker},
};
use embassy_sync::{blocking_mutex::raw::NoopRawMutex, channel::Channel as Queue};

mod event;
mod impls;
mod request;
mod target;

use event::Event;
pub use impls::channel;

/// A local handle. The queue and state stay on the executor where the owner runs.
pub struct Handle<S, const N: usize, E = core::convert::Infallible, Role = ()> {
    inner: Rc<Inner<S, N, E>>,
    role: PhantomData<fn() -> Role>,
}

pub struct WeakHandle<S, const N: usize, E = core::convert::Infallible, Role = ()> {
    inner: Weak<Inner<S, N, E>>,
    role: PhantomData<fn() -> Role>,
}

/// Move this into one application task and await run or run_with there.
pub struct Owner<S, const N: usize, E = core::convert::Infallible> {
    inner: Rc<Inner<S, N, E>>,
}

pub struct Completion<E> {
    inner: Rc<Completed<E>>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct Request<'a, S, const N: usize, E, O, Role = ()> {
    handle: &'a Handle<S, N, E, Role>,
    message: Option<Message<S>>,
    reply: Reply<O>,
    closed: event::Listener<'a>,
    submitted: bool,
}

#[must_use = "await the checked request to submit it and receive its result"]
pub struct CheckedRequest<'a, S, const N: usize, E, O, Role = ()>(Request<'a, S, N, E, O, Role>);

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O> {
    answer: Rc<Answer<O>>,
}

#[must_use = "await the checked reply to receive the operation's result"]
pub struct CheckedReply<O>(Reply<O>);

pub type Channel<S, const N: usize, E = core::convert::Infallible> =
    (Handle<S, N, E>, Owner<S, N, E>);

#[derive(Debug)]
pub enum OwnerError<E> {
    Setup(E),
    Cleanup(E),
    Cancelled,
}

struct Inner<S, const N: usize, E> {
    queue: Queue<NoopRawMutex, Message<S>, N>,
    open: Cell<bool>,
    handles: Cell<usize>,
    closed: Event,
    completion: Rc<Completed<E>>,
}

struct Completed<E> {
    ready: RefCell<Option<Result<(), Rc<OwnerError<E>>>>>,
    result: RefCell<Option<Result<(), Rc<OwnerError<E>>>>>,
    changed: Event,
}

struct Message<S> {
    _operation: Operation,
    job: Box<dyn Job<S>>,
}

trait Job<S> {
    fn run<'s>(&'s mut self, state: &'s mut S) -> LocalFuture<'s, ()>;
}

struct Call<F, I, O> {
    function: Option<(F, I)>,
    answer: Rc<Answer<O>>,
}

struct Answer<O> {
    result: RefCell<AnswerState<O>>,
}

enum AnswerState<O> {
    Waiting(Option<Waker>),
    Ready(Result<O, CallError>),
    Abandoned,
    Consumed,
}
