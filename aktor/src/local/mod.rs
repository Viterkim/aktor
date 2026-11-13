use crate::AktorError;
use crate::{
    message::{AktorStateError, CallError, LocalFuture},
    operation::Operation,
};
use alloc::{
    boxed::Box,
    collections::VecDeque,
    rc::{Rc, Weak},
    string::String,
};
use core::{
    cell::{Cell, RefCell},
    future::Future,
    marker::PhantomData,
    task::{Poll, Waker},
};

pub mod clock;
mod dispatch;
mod event;
mod group;
pub mod hooks;
mod impls;
mod latest;
mod opaque_dispatch;
#[cfg(feature = "std")]
#[doc(hidden)]
pub mod panic;
mod request;
mod signal;
#[cfg(feature = "embassy_cross_core")]
pub(crate) use group::OwnerStatus;
pub use group::{
    AktorArgs, AktorGroup, AktorGroupCompletion, AktorKillSwitch, AktorShutdownReport,
};
#[doc(hidden)]
pub use impls::runner::serve;
#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
#[doc(hidden)]
pub use impls::runner::serve_browser;
#[doc(hidden)]
pub use impls::runner::serve_on;
pub use impls::runner::{AktorCustomCall, AktorRunner};
pub use latest::{LatestResults, LatestSender};
use signal::StopSignal;

#[cfg(any(
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
pub use crate::setup::definition::LocalDefinition as AktorDefinition;
use event::Event;
pub use impls::{channel, channel_with_capacity, channel_with_clock};

/// A local handle. The queue and state stay on the executor where the owner runs.
pub struct Handle<S, const N: usize, E = core::convert::Infallible, Role = (), Clock = ()> {
    inner: Rc<Inner<S, N, E>>,
    role: PhantomData<fn() -> (Role, Clock)>,
}

pub struct WeakHandle<S, const N: usize, E = core::convert::Infallible, Role = (), Clock = ()> {
    inner: Weak<Inner<S, N, E>>,
    role: PhantomData<fn() -> (Role, Clock)>,
}

/// Move this into one application task and await run or run_with there.
/// Keep a completion first if you want the original typed failure data.
#[doc(hidden)]
pub struct Owner<S, const N: usize, E = core::convert::Infallible> {
    pub hooks: hooks::CallHooks<S>,
    inner: Rc<Inner<S, N, E>>,
}

#[doc(hidden)]
pub struct Completion<E> {
    inner: Rc<Completed<E>>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct Request<'a, S, const N: usize, E, O, Role = (), Clock = ()> {
    handle: &'a Handle<S, N, E, Role, Clock>,
    message: Option<Message<S>>,
    reply: Reply<O, Clock>,
    space: Option<event::Listener<'a>>,
    submitted: bool,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O, Clock = ()> {
    clock: PhantomData<fn() -> Clock>,
    parked: bool,
    taken: bool,
    group: Option<(Rc<str>, StopSignal)>,
    answer: Rc<Answer<O>>,
}

#[doc(hidden)]
pub type Channel<S, const N: usize, E = core::convert::Infallible, Clock = ()> =
    (Handle<S, N, E, (), Clock>, Owner<S, N, E>);

#[derive(er::Er)]
#[er(
    no_constructors,
    display = Self::fmt_display,
    display_alt = Self::fmt_details,
    debug = Self::fmt_display
)]
pub enum OwnerError<E = ()> {
    Setup(AktorError<E>),
    SetupDiagnostic(crate::AktorError, bool),
    SetupPanic(crate::AktorError),
    Cleanup(AktorError<E>),
    CleanupFailure(crate::AktorFailure),
    CleanupUnconfirmed,
    Runner(crate::AktorError),
    Cancelled,
}

/// The original local lifecycle error, shared between typed observers.
#[derive(er::Er)]
#[er(no_constructors, format = "{cause}")]
#[non_exhaustive]
pub struct SharedOwnerError<E> {
    pub cause: Rc<OwnerError<E>>,
}

struct Inner<S, const N: usize, E> {
    queue: RefCell<VecDeque<Message<S>>>,
    services: RefCell<VecDeque<Message<S>>>,
    prefer_service: Cell<bool>,
    group: RefCell<Option<(Rc<str>, StopSignal)>>,
    sessions: RefCell<alloc::vec::Vec<Weak<dyn Fn() -> bool>>>,
    prune_at: Cell<usize>,
    open: Cell<bool>,
    /// Shutdown gave up on the calls, `end` still runs.
    cancelled: Cell<bool>,
    handles: Cell<usize>,
    capacity: usize,
    owner_waker: RefCell<Option<Waker>>,
    space: Event,
    closed: Event,
    completion: Rc<Completed<E>>,
}

struct Completed<E> {
    cleanup_completed: Cell<bool>,
    ready: RefCell<Option<Result<(), Rc<OwnerError<E>>>>>,
    result: RefCell<Option<Result<(), Rc<OwnerError<E>>>>>,
    diagnostics: Rc<RefCell<alloc::vec::Vec<crate::AktorError>>>,
    panic_reported: Cell<bool>,
    /// Shutdown's cancel stopped it while calls were still going.
    cut_call: Cell<bool>,
    /// `end` used up its grace and got dropped.
    ran_out: Cell<bool>,
    changed: Event,
}

struct CompletionGuard<'a, S, const N: usize, E> {
    inner: &'a Inner<S, N, E>,
    failure: Rc<OwnerError<E>>,
    armed: bool,
}

struct Message<S> {
    _operation: Operation,
    job: Box<dyn Job<S>>,
}

trait Job<S> {
    fn service_id(&self) -> Option<*const ()> {
        None
    }

    fn run<'s>(
        &'s mut self,
        state: &'s mut S,
        hooks: &'s mut hooks::CallHooks<S>,
        operation: Operation,
    ) -> LocalFuture<'s, ()>;
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
