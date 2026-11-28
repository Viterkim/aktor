pub mod clock;
pub mod hooks;
#[cfg(feature = "std")]
#[doc(hidden)]
pub mod panic;

mod dispatch;
pub(crate) mod event;
pub(crate) use event::wake;
mod group;
mod impls;
mod latest;
mod opaque_dispatch;
mod request;
mod signal;

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
#[cfg(feature = "embassy_cross_core")]
pub(crate) use group::OwnerStatus;
pub use group::{
    AktorArgs, AktorGroup, AktorGroupCompletion, AktorKillSwitch, AktorShutdownReport,
};
pub(crate) use impls::closed_channel;
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
pub use impls::{channel, channel_with_clock};
pub use latest::{LatestResults, LatestSender};
pub use request::{RequestCast, RequestSend};

use crate::{
    message::{AktorDeliveryOutcome, AktorStateError, LocalFuture},
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
use er::*;
use event::Event;
pub(crate) use signal::StopSignal;

/// A local handle. The queue and state stay on the executor where the owner runs.
pub struct Handle<S, Role = (), Clock = ()> {
    inner: Rc<Inner<S>>,
    role: PhantomData<fn() -> (Role, Clock)>,
}

pub struct WeakHandle<S, Role = (), Clock = ()> {
    inner: Weak<Inner<S>>,
    role: PhantomData<fn() -> (Role, Clock)>,
}

/// Move this into one application task and await run or run_with there.
/// Keep a completion first if you want the original typed failure data.
#[doc(hidden)]
pub struct Owner<S, E = core::convert::Infallible> {
    pub hooks: hooks::CallHooks<S>,
    inner: Rc<OwnerState<S, E>>,
}

#[doc(hidden)]
pub struct Completion<E> {
    inner: Rc<Completed>,
    result: OwnerResult<E>,
}

#[must_use = "await the request, or explicitly send or cast it"]
pub struct Request<'a, S, O, Role = (), Clock = ()> {
    handle: &'a Handle<S, Role, Clock>,
    message: Option<Message<S>>,
    reply: Reply<O, Clock>,
    space: Option<event::Listener<'a>>,
    submitted: bool,
}

#[must_use = "await the reply to receive the operation's result"]
pub struct Reply<O, Clock = ()> {
    clock: PhantomData<fn() -> Clock>,
    taken: bool,
    completion: Rc<Completed>,
    answer: Rc<Answer<O>>,
}

#[doc(hidden)]
pub type Channel<S, E = core::convert::Infallible, Clock = ()> =
    (Handle<S, (), Clock>, Owner<S, E>);

#[derive(Er)]

pub enum OwnerError<E = ()> {
    #[er(format = "actor setup failed")]
    Setup(#[er(skip)] E),
    #[er(format = "actor setup failed")]
    SetupDiagnostic { id: crate::FailureId },
    #[er(format = "actor setup panicked")]
    SetupPanic,
    #[er(format = "actor cleanup failed")]
    Cleanup(#[er(skip)] E),
    #[er(format = "actor cleanup failed")]
    CleanupFailure { id: Option<crate::FailureId> },
    #[er(format = "actor stopped before cleanup completed")]
    CleanupUnconfirmed,
    #[er(format = "actor runner failed")]
    Runner,
    #[er(format = "actor owner stopped before cleanup completed")]
    Cancelled,
}

#[derive(Er)]
pub enum RunnerError {
    Failed,
    #[er(format = "custom runner stopped before draining admission")]
    StoppedBeforeDraining,
    #[er(format = "custom runner discarded admitted work")]
    DiscardedCall,
}

type Observed<E> = (Rc<OwnerError<E>>, String);

/// The original local lifecycle error, shared between typed observers.
#[derive(Er)]
#[er(format = "{cause}")]
#[non_exhaustive]
pub struct SharedOwnerError<E> {
    pub cause: Rc<OwnerError<E>>,
}

type OwnerResult<E> = Rc<RefCell<Option<Result<(), Observed<E>>>>>;

struct OwnerState<S, E> {
    mailbox: Rc<Inner<S>>,
    result: OwnerResult<E>,
}
impl<S, E> core::ops::Deref for OwnerState<S, E> {
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.mailbox
    }
}

struct Inner<S> {
    queue: RefCell<VecDeque<Message<S>>>,
    services: RefCell<VecDeque<Message<S>>>,
    prefer_service: Cell<bool>,
    group: RefCell<Option<(Rc<str>, crate::AktorExecution, StopSignal)>>,
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
    completion: Rc<Completed>,
}

struct Completed {
    lost: core::cell::OnceCell<crate::Delivery>,
    refused: core::cell::OnceCell<crate::Delivery>,
    returned: RefCell<alloc::vec::Vec<crate::Delivery>>,
    init: Cell<crate::AktorInitEvidence>,
    cleanup_completed: Cell<bool>,
    ready: RefCell<Option<Result<(), Observed<()>>>>,
    result: OwnerResult<()>,
    diagnostics: Rc<RefCell<alloc::vec::Vec<String>>>,
    /// Shutdown's cancel stopped it while calls were still going.
    cut_call: Cell<bool>,
    /// `end` used up its grace and got dropped.
    ran_out: Cell<bool>,
    changed: Event,
}

struct CompletionGuard<'a, S, E> {
    inner: &'a OwnerState<S, E>,
    failure: Option<Observed<E>>,
    armed: bool,
}

struct Message<S> {
    operation: Operation,
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
    completion: Rc<Completed>,
    function: Option<(F, I)>,
    answer: Rc<Answer<O>>,
}

struct Answer<O> {
    result: RefCell<AnswerState<O>>,
}

enum AnswerState<O> {
    Waiting(Option<Waker>),
    Ready(Result<O, crate::CallLoss>),
    Abandoned,
    Consumed,
}
