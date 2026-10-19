#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
use crate::listener::DedicatedStartError;
use crate::{AktorNoRole, AktorSetupError};
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{future::Future, pin::Pin, time::Duration};
use er::Er;

#[cfg(feature = "bevy")]
mod bevy;
pub mod closures;
#[cfg(feature = "embassy_cross_core")]
mod cross_core;
#[cfg(feature = "local")]
mod custom;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
mod definition;
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
pub use worker::{AktorWorkerNew, AktorWorkerOptions};

pub use closures::{AktorClosure, AktorIntervalLogic};
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub use definition::AktorDefinition;
use group::AktorSetupGroup;
pub use impls::{aktor_start, aktor_start_in};
#[doc(hidden)]
pub use impls::{start_diagnostics, validation_error};
/// Setup choices, each with its own type and executor requirements.
pub use kind as AktorKind;
pub use kind::{AktorExecution, AktorMode};

pub use crate::types::{
    AktorClosures, AktorInterval, AktorName, AktorNew, AktorNewOptions, AktorOptions, AktorSetup,
    AktorStarted,
};

#[must_use = "await startup to get the actor handles"]
pub struct AktorStartup<Setup: AktorStart<Mode>, Mode = ()> {
    mode: core::marker::PhantomData<fn() -> Mode>,
    pub killswitch: <Setup::Group as AktorSetupGroup>::KillSwitch,
    pub completion: <Setup::Group as AktorSetupGroup>::Completion,
    begin: bool,
    group: Option<Setup::Group>,
    setup: Option<Box<Setup>>,
    starting: Option<Pin<Box<Setup::Startup>>>,
    failure: Option<Box<AktorStartError>>,
    shutdown: Option<Pin<Box<<Setup::Group as AktorSetupGroup>::Closing>>>,
}

/// The startup failure and its completed rollback report.
/// report is None if startup was rejected before adding actors to the group.
#[derive(Er)]
#[er(no_constructors, format = "actor startup failed", display_alt = Self::fmt_details)]
pub struct AktorStartupError {
    pub error: AktorStartError,
    #[er(source, skip)]
    pub diagnostics: Box<crate::AktorDiagnostic>,
    pub report: Option<Box<crate::ShutdownReport>>,
    /// The startup diagnostic represents the group's first failure.
    pub source_is_failure: bool,
}

#[derive(Er)]
pub enum AktorStartError<E: 'static = crate::AktorFailure> {
    Init(#[er(source, skip)] E),
    Setup(#[er(source, skip)] AktorSetupError),
    #[cfg(feature = "local")]
    Local(#[er(source, skip)] crate::local::OwnerError),
    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    Thread(#[er(source, skip)] DedicatedStartError<core::convert::Infallible>),
}

#[doc(hidden)]
pub trait AktorStart<Mode = ()>: Sized {
    type Group: AktorSetupGroup;
    type Handles;
    type Startup: Future<Output = Result<Self::Handles, AktorStartError>>;

    fn source_is_setup(error: &AktorStartError) -> bool {
        error.source_is_setup()
    }

    fn diagnostics(error: &AktorStartError) -> String {
        impls::start_diagnostics(error)
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError>;
    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup;
}

#[doc(hidden)]
pub struct AktorStartContext<Group: AktorSetupGroup> {
    group: Group,
}

pub type AktorDiagnostics = String;

pub type AktorThreadFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
type AktorClosingFuture = AktorThreadFuture<crate::ShutdownReport>;
#[cfg(all(
    any(feature = "browser_local", feature = "wasm_browser_workers"),
    target_family = "wasm"
))]
type AktorClosingFuture = crate::message::LocalFuture<'static, crate::ShutdownReport>;
