#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
use crate::AktorCleanupError;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
use crate::listener::DedicatedStartError;
use crate::{AktorNoRole, AktorSetupError};
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{future::Future, pin::Pin, time::Duration};
use er::Er;

pub mod closures;
#[doc(hidden)]
pub mod group;
mod impls;
use group::AktorSetupGroup;
#[cfg(feature = "bevy")]
mod bevy;
#[cfg(feature = "embassy_cross_core")]
mod cross_core;
#[cfg(feature = "local")]
mod custom;
#[cfg(feature = "local")]
mod driven;
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
mod startup;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod task;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
mod thread;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub use thread::AktorThreadStartError;
mod tuple;
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
mod worker;
#[doc(hidden)]
pub use tuple::{AktorPairStartError, AktorTupleStartup};
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use worker::{AktorWorkerOptions, AktorWorkerSetup};

pub use closures::{AktorClosure, AktorEnd, AktorIntervalLogic};
pub use impls::{start, start_in};
/// Setup choices, each with its own type and executor requirements.
pub use kind as AktorKind;
pub use kind::{AktorExecution, AktorLifecycle, AktorMode};

pub use crate::types::{
    AktorClosures, AktorInterval, AktorName, AktorOptions, AktorSetup, AktorStarted,
};

#[must_use = "await startup to get the actor handles"]
pub struct AktorStartup<Setup: AktorStart> {
    pub killswitch: <Setup::Group as AktorSetupGroup>::KillSwitch,
    pub completion: <Setup::Group as AktorSetupGroup>::Completion,
    begin: bool,
    group: Option<Setup::Group>,
    setup: Option<Box<Setup>>,
    starting: Option<Pin<Box<Setup::Startup>>>,
    failure: Option<Box<Setup::Error>>,
    shutdown: Option<Pin<Box<<Setup::Group as AktorSetupGroup>::Closing>>>,
}

/// The original startup error and its completed rollback report.
/// report is None if startup was rejected before adding actors to the group.
#[derive(Er)]
#[er(format = "{error}", no_constructors)]
pub struct AktorStartupError<E: core::error::Error + 'static> {
    #[er(source)]
    pub error: E,
    pub report: Option<Box<crate::ShutdownReport>>,
}

#[derive(Er)]
pub enum AktorStartError {
    #[er(format = "{0}")]
    Setup(#[er(source)] AktorSetupError),
    #[er(format = "{0}")]
    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    Thread(#[er(source)] DedicatedStartError<AktorSetupError>),
}

#[doc(hidden)]
pub trait AktorStart: Sized {
    type Group: AktorSetupGroup;
    type Handles;
    type Error: core::error::Error + From<AktorSetupError> + 'static;
    type Startup: Future<Output = Result<Self::Handles, Self::Error>>;

    fn grace(&self) -> Duration;
    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError>;
    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup;
}

#[doc(hidden)]
pub struct AktorStartContext<Group: AktorSetupGroup> {
    group: Group,
}

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
