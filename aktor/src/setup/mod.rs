// Rebuild an `AktorNew` whose `start` returns anything that turns into `(E, String)` into one whose
// `start` returns the pair, for the launch code underneath. A macro, because a method returning
// `impl AsyncFnOnce` would also capture `Kind`, and a kind with a lifetime then isn't `'static`.
macro_rules! pair_start {
    ($setup:expr, $convert:path, $end:ty) => {{
        let crate::AktorNew {
            name,
            kind,
            start,
            end,
            hooks,
            capacity,
            stuck,
        } = $setup;

        crate::AktorNew::<_, _, _, _, $end, _, _> {
            name,
            kind,
            start: $convert(start.0),
            end,
            hooks: crate::AktorHooks {
                intervals: hooks.intervals,
                before_each: crate::setup::closures::EachHook::boxed(hooks.before_each),
                after_each: crate::setup::closures::EachHook::boxed(hooks.after_each),
            },
            capacity,
            stuck,
        }
    }};
}

pub mod closures;
#[doc(hidden)]
pub mod group;
#[doc(hidden)]
pub mod initial;
pub mod kind;
#[cfg(feature = "local")]
pub(crate) mod ready;
pub mod shutdown;

#[cfg(feature = "bevy")]
mod bevy;
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
mod impls;
#[cfg(feature = "local")]
mod intervals;
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
pub(crate) mod receipt;
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

pub use crate::types::{
    AktorHooks, AktorInterval, AktorNew, AktorSetup, AktorSpawned, AktorStarted,
};
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
pub use impls::aktor_start;
pub(crate) use impls::validation_error;
pub use initial::{AktorBranch, AktorInit};
#[cfg(all(
    feature = "wasm_browser_worker_server",
    target_family = "wasm",
    target_os = "unknown"
))]
pub(crate) use initial::{ReadyState, TryState};
/// Choose where the actor runs.
pub use kind as AktorKind;
pub use kind::{AktorExecution, AktorMode};
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use worker::{AktorWorkerNew, Optional, Required};

use crate::AktorGroupError;
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{future::Future, pin::Pin, time::Duration};
use er::*;
use group::AktorSetupGroup;

/// Every launched actor has initialization evidence and a rollback outcome.
#[derive(Er)]
pub enum AktorStartError<E = core::convert::Infallible> {
    #[er(format = "actor startup refused: {reason}")]
    Refused { reason: AktorStartReason },
    #[er(format = "actor startup failed")]
    Initializer {
        #[er(skip)]
        error: E,
        #[er(skip)]
        startup: AktorStartRollback,
    },
    #[er(format = "actor startup failed: {reason}")]
    Failed {
        reason: AktorStartReason,
        #[er(skip)]
        startup: AktorStartRollback,
    },
}

impl<E> AktorStartError<E> {
    pub fn startup(&self) -> Option<&AktorStartRollback> {
        match self {
            Self::Refused { .. } => None,
            Self::Initializer { startup, .. } | Self::Failed { startup, .. } => Some(startup),
        }
    }

    pub fn reason(&self) -> AktorStartReason {
        match self {
            Self::Refused { reason } | Self::Failed { reason, .. } => *reason,
            Self::Initializer { .. } => AktorStartReason::Init,
        }
    }
}

#[derive(Debug)]
pub struct AktorStartRollback {
    pub actors: Vec<crate::AktorActorInit>,
    pub failure_id: Option<crate::FailureId>,
    pub report: Result<Box<crate::AktorShutdownReport>, crate::AktorPublicationLost>,
}

#[doc(hidden)]
#[derive(Er)]
#[er(format = "actor startup failed")]
#[non_exhaustive]
pub struct LaunchError<E = core::convert::Infallible> {
    /// The initializer error is shared by every waiter.
    #[er(skip)]
    pub init: Option<E>,
    pub reason: AktorStartReason,
    /// The failure in the group report that this startup receipt describes.
    #[er(skip)]
    pub failure_id: Option<crate::FailureId>,
}

#[derive(Clone, Copy, PartialEq, Eq, Er)]
pub enum AktorStartReason {
    Init,
    Stopping,
    InvalidSetup,
}

#[doc(hidden)]
pub type StartFailure<E = core::convert::Infallible> = (LaunchError<E>, String);

/// Use an [`AktorNew`], a native `AktorLoop`, a browser worker or a tuple of them in `actors`.
pub trait AktorStart: Sized {
    type Group: AktorSetupGroup;
    type Handles;
    type Error: 'static;

    #[doc(hidden)]
    fn needs_tokio_group(&self) -> bool {
        false
    }

    #[doc(hidden)]
    fn actor_name(&self) -> &str;
    #[doc(hidden)]
    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)>;

    /// Handles for an actor that never launched, every call on them finds it gone.
    #[doc(hidden)]
    fn closed(name: &str, group: &Self::Group) -> Self::Handles;

    #[doc(hidden)]
    fn fallback(&self) -> impl FnOnce(&Self::Group) -> Self::Handles + use<Self> {
        let name = String::from(self.actor_name());
        move |group| Self::closed(&name, group)
    }

    #[doc(hidden)]
    fn launch(self, context: AktorStartContext<Self::Group>) -> AktorLaunch<Self>;
}

#[cfg(feature = "local")]
#[derive(Er)]
enum LocalSetupErr {
    Stopped,
}

pub(crate) type AktorReady<Group, E> = <Group as AktorSetupGroup>::Ready<E>;

pub type AktorLaunch<Setup> = Result<
    (
        <Setup as AktorStart>::Handles,
        AktorReady<<Setup as AktorStart>::Group, <Setup as AktorStart>::Error>,
    ),
    StartFailure<<Setup as AktorStart>::Error>,
>;

#[doc(hidden)]
pub struct AktorStartContext<Group: AktorSetupGroup> {
    pub group: Group,
}

pub type AktorThreadFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_os = "unknown",
    target_feature = "atomics"
))]
mod shared;

pub(crate) type Prepared<S, Start, Kind, End, Signature> = AktorNew<
    S,
    Start,
    Kind,
    End,
    Signature,
    Option<Box<<Kind as AktorMode>::Each<S>>>,
    Option<Box<<Kind as AktorMode>::Each<S>>>,
>;
