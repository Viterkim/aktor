#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
use crate::listener::DedicatedStartError;
use crate::{AktorError, AktorGroupError};
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{future::Future, pin::Pin, time::Duration};
use er::Er;
#[doc(hidden)]
pub mod initial;
use initial::InitError;
pub use initial::{AktorBranch, aktor_ready};

#[cfg(feature = "bevy")]
mod bevy;
pub mod closures;
#[cfg(feature = "embassy_cross_core")]
mod cross_core;
#[cfg(feature = "local")]
mod custom;
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
pub(crate) mod definition;
#[cfg(feature = "local")]
mod driven;
#[doc(hidden)]
pub mod group;
mod impls;
#[cfg(feature = "local")]
mod intervals;
pub mod kind;
#[cfg(all(
    feature = "local",
    any(
        all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")),
        all(
            feature = "browser_local",
            target_family = "wasm",
            target_os = "unknown"
        )
    )
))]
mod local;
#[cfg(feature = "local")]
pub mod ready;
pub mod shutdown;
mod startup;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod task;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
mod thread;
mod tuple;
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
mod worker;
#[doc(hidden)]
pub use tuple::AktorTupleStartup;
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use worker::{AktorOptionalWorkerNew, AktorWorkerNew, AktorWorkerOptions};

pub(crate) use crate::report::diagnostics;
pub use closures::{AktorClosure, AktorIntervalLogic};
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
pub use definition::AktorDefinition;
use group::AktorSetupGroup;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub use impls::aktor_start_with_policy;
pub use impls::{aktor_start, aktor_start_in};
#[doc(hidden)]
pub use impls::{start_diagnostics, validation_error};
/// Choose where the worker runs.
pub use kind as AktorKind;
pub use kind::{AktorExecution, AktorMode};

pub use crate::types::{AktorHooks, AktorInterval, AktorNew, AktorSetup, AktorStarted};

type Closing<Group> =
    <<Group as AktorSetupGroup>::Completion as core::future::IntoFuture>::IntoFuture;

#[must_use = "await startup to get the actor handles"]
pub struct AktorStartup<Setup: AktorStart> {
    pub killswitch: <Setup::Group as AktorSetupGroup>::AktorKillSwitch,
    pub completion: <Setup::Group as AktorSetupGroup>::Completion,
    begin: bool,
    group: Option<Setup::Group>,
    setup: Option<Box<Setup>>,
    starting: Option<Pin<Box<Setup::Startup>>>,
    failure: Option<Box<AktorStartError<Setup::Error>>>,
    shutdown: Option<Pin<Box<Closing<Setup::Group>>>>,
}

/// What happened to the initializers in this startup attempt.
#[cfg_attr(feature = "transport", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AktorInitOutcome<E> {
    /// None of them ran.
    NotRun,
    /// An initializer reported this error. A worker can report it while rollback is still running.
    Failed(E),
    /// An initializer may have run. It may have acquired resources before startup stopped.
    Unconfirmed,
}

/// A failed start. `aktor_start` returns this error with the report text. Use `.er()` to keep both.
/// `report` is `None` when there's no rollback report, for example when `group.spawn`
/// gets called after the group closes.
#[derive(Er)]
#[er(no_constructors, format = "actor startup failed")]
#[non_exhaustive]
pub struct AktorStartError<E = core::convert::Infallible> {
    #[er(skip)]
    pub init: AktorInitOutcome<E>,
    pub cause: AktorStartCause,
    pub report: Option<Box<crate::AktorShutdownReport>>,
}

#[derive(Er)]
#[er(no_constructors)]
pub enum AktorStartCause<E: 'static = crate::AktorFailure> {
    Init(#[er(source, skip)] E),
    Setup(#[er(source, skip)] AktorError),
    Group(#[er(source, skip)] AktorGroupError),
    #[cfg(feature = "local")]
    Local(
        #[er(source, skip)] crate::local::OwnerError,
        #[er(skip)] String,
    ),
    #[cfg(all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    Worker(
        #[er(source, skip)] crate::worker::WorkerError,
        #[er(skip)] String,
    ),
    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    Thread(#[er(source, skip)] DedicatedStartError<core::convert::Infallible>),
    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    Startup(#[er(source, skip)] Box<crate::owner::StartError>),
}

#[doc(hidden)]
pub trait AktorStart: Sized {
    type Group: AktorSetupGroup;
    type Handles;
    type Error;
    type Startup: Future<Output = Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn source_is_setup(error: &AktorStartCause) -> bool {
        error.source_is_setup()
    }

    fn diagnostics(error: &AktorStartCause) -> String {
        impls::start_diagnostics(error)
    }

    fn needs_tokio_group(&self) -> bool {
        false
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause>;
    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup;
}

#[doc(hidden)]
pub struct AktorStartContext<Group: AktorSetupGroup> {
    group: Group,
}

pub type AktorThreadFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_os = "unknown",
    target_feature = "atomics"
))]
mod shared;
