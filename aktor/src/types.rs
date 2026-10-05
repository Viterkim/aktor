use alloc::{string::String, vec::Vec};

/// The execution kind recorded in reports. Setup constructors live in AktorKind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorExecution {
    TokioThread,
    TokioTask,
    TokioLocal,
    StdThread,
    Local,
    BrowserLocal,
    BrowserWebWorker,
    EmbassyLocal,
    EmbassyCrossCore,
    BevyLocal,
    BevyTask,
    Custom,
}

pub type AktorNoRole = ();

#[allow(non_upper_case_globals)]
pub const AktorNoRole: AktorNoRole = ();

/// The text you want printed, plus any data you want to keep.
#[cfg_attr(
    feature = "wasm_browser_workers",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, PartialEq, Eq)]
pub struct AktorError<T = ()> {
    pub diagnostics: String,
    pub data: T,
}

pub type AktorSetupError<T = ()> = AktorError<T>;
pub type AktorCleanupError<T = ()> = AktorError<T>;

/// Setup and cleanup for an actor owned by the application.
pub struct ActorArgs<Setup, Cleanup> {
    pub name: String,
    pub capacity: usize,
    pub setup: Setup,
    pub cleanup: Cleanup,
}

/// What failed and how cleanup went.
#[derive(Clone, Debug, Default)]
pub struct ShutdownReport {
    pub failure: Option<ActorFailure>,
    pub actors: Vec<ActorOutcome>,
    pub application: Vec<AktorError>,
    pub timed_out: bool,
}

#[derive(Clone, Debug)]
pub struct ActorFailure {
    pub actor: String,
    pub kind: Option<AktorExecution>,
    pub phase: String,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct ActorOutcome {
    pub actor: String,
    pub kind: Option<AktorExecution>,
    pub diagnostics: Vec<AktorCleanupError>,
    pub timed_out: bool,
}

#[cfg(any(
    feature = "local",
    all(
        any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
mod prepared {
    use super::AktorNoRole;
    use crate::setup::{AktorMode, group::AktorSetupGroup};
    use alloc::{string::String, vec::Vec};
    use core::time::Duration;

    pub use crate::setup::{
        AktorClosure, AktorKind, AktorLifecycle, AktorStartError, AktorStartup, AktorStartupError,
    };

    pub struct AktorClosures<S: 'static, Start, Kind: AktorMode> {
        pub start: Start,
        pub end: Option<AktorClosure<Kind::End<S>>>,
        pub intervals: Vec<AktorInterval<S, Kind>>,
        pub before_each: Option<AktorClosure<Kind::Each<S>>>,
        pub after_each: Option<AktorClosure<Kind::Each<S>>>,
    }

    pub struct AktorInterval<S: 'static, Kind: AktorMode> {
        pub every: Duration,
        pub run: AktorClosure<Kind::Interval<S>>,
    }

    pub struct AktorName {
        pub name: String,
    }
    pub struct AktorSetup<S: 'static, Start, Kind: AktorMode, Role = AktorNoRole> {
        pub name: AktorName,
        pub role: Role,
        pub kind: Kind,
        pub closures: AktorClosures<S, Start, Kind>,
        pub options: Option<AktorOptions>,
    }

    pub struct AktorOptions {
        pub capacity: usize,
        pub shutdown_grace: Duration,
    }

    /// Keep this owner alive while using its handles. Dropping its group starts shutdown.
    ///
    /// ```rust,ignore
    /// let actors = start(setup).await?;
    /// let count = count_users(&actors.handles).await;
    /// let report = actors.shutdown().await;
    /// ```
    #[must_use = "keep the startup owner alive while using its handles"]
    pub struct AktorStarted<Handles, Group: AktorSetupGroup> {
        pub handles: Handles,
        pub group: Group,
    }
}
#[cfg(any(
    feature = "local",
    all(
        any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
pub use prepared::*;
