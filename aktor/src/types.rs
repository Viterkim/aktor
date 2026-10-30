use alloc::{boxed::Box, string::String, vec::Vec};

/// The execution kind recorded in reports. Setup constructors live in AktorKind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorExecution {
    TokioThread,
    TokioTask,
    TokioLocal,
    StdThread,
    Local,
    BrowserLocal,
    BrowserThread,
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

#[cfg_attr(feature = "transport", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, PartialEq, Eq, er::Er)]
#[er(format = "{message}", display_alt = "{report}")]
pub struct AktorDiagnostic {
    pub message: String,
    #[er(into_report_string)]
    pub report: String,
}

#[derive(Clone, er::Er)]
#[er(no_constructors, format = "actor group failed", display_alt = Self::fmt_details)]
pub struct AktorRunError {
    pub report: Box<ShutdownReport>,
    #[er(source, skip)]
    pub diagnostics: AktorDiagnostic,
}

#[derive(Clone, er::Er)]
#[er(no_constructors, format = "{message}", display_alt = Self::fmt_details)]
pub struct AktorGroupError {
    pub message: String,
    #[er(source, skip)]
    pub diagnostics: Option<AktorDiagnostic>,
}

/// Setup and cleanup diagnostics, with optional data for low-level APIs.
#[cfg_attr(feature = "transport", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, PartialEq, Eq, er::Er)]
#[er(format = "actor lifecycle failed", display_alt = "{diagnostics}")]
pub struct AktorError<T = ()> {
    #[er(into_report_string)]
    pub diagnostics: String,
    #[er(default)]
    pub data: T,
}

pub type AktorSetupError<T = ()> = AktorError<T>;
pub type AktorCleanupError<T = ()> = AktorError<T>;

#[doc(hidden)]
#[derive(Clone, er::Er)]
#[er(
    no_constructors,
    format = "actor lifecycle failed",
    display_alt = "{diagnostics}"
)]
pub struct AktorFailure {
    pub diagnostics: String,
    #[er(skip)]
    pub primary: bool,
}

pub type AktorCleanupFailure = AktorFailure;

pub trait AktorCleanupOutput {
    fn into_cleanup(self) -> Result<(), AktorCleanupError>;
}
#[doc(hidden)]
pub struct AktorShutdownSync;
#[doc(hidden)]
pub struct AktorShutdownAsync;

#[doc(hidden)]
pub trait AktorShutdownOutput<Mode> {
    type Future: core::future::Future<Output = Result<(), AktorCleanupError>>;

    fn into_shutdown(self) -> Self::Future;
}

#[doc(hidden)]
pub struct AktorShutdownFuture<F> {
    pub future: core::pin::Pin<alloc::boxed::Box<F>>,
}

/// Setup and cleanup for an actor owned by the application.
pub struct AktorArgs<Setup, Cleanup> {
    pub name: String,
    pub capacity: usize,
    pub setup: Setup,
    pub cleanup: Cleanup,
}

/// What failed and how cleanup went.
#[derive(Clone, Debug, Default)]
pub struct ShutdownReport {
    /// Stopped during root startup, or because a new actor could not initialize.
    pub startup: bool,
    pub failure: Option<AktorFailureReport>,
    pub actors: Vec<AktorOutcome>,
    pub application: Vec<AktorError>,
    pub timed_out: bool,
}

#[derive(Clone, Debug)]
pub struct AktorFailureReport {
    pub actor: String,
    pub kind: Option<AktorExecution>,
    pub phase: String,
    pub message: String,
}

/// Whether an owner finished its managed work and cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetirementStatus {
    Confirmed,
    Unconfirmed,
}

#[derive(Clone, Debug)]
pub struct AktorOutcome {
    pub retirement: RetirementStatus,
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
    use super::{AktorNoRole, ShutdownReport};
    use crate::setup::{AktorMode, group::AktorSetupGroup};
    use alloc::{string::String, vec::Vec};
    use core::time::Duration;

    pub use crate::setup::{
        AktorClosure, AktorKind, AktorStartError, AktorStartup, AktorStartupError,
    };

    pub struct AktorClosures<S: 'static, Start, Kind: AktorMode, End = ()> {
        pub start: Start,
        pub end: End,
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

    pub struct AktorNew<S: 'static, Start, Kind: AktorMode, Role = AktorNoRole, End = ()> {
        pub name: AktorName,
        pub role: Role,
        pub kind: Kind,
        pub closures: AktorClosures<S, Start, Kind, End>,
        pub options: AktorNewOptions,
    }

    pub struct AktorNewOptions {
        pub capacity: usize,
    }

    pub struct AktorSetup<Setups, Shutdown, ShutdownOutput>
    where
        Shutdown: FnOnce(ShutdownReport) -> ShutdownOutput,
    {
        pub actors: Setups,
        pub shutdown: Shutdown,
        pub options: AktorOptions,
    }

    pub struct AktorOptions {
        pub shutdown_grace: Duration,
    }

    /// Keep this owner alive while using its handles. Dropping its group starts shutdown.
    ///
    /// ```rust,ignore
    /// let actors = aktor_start(setup).await?;
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

#[cfg(feature = "transport")]
pub use crate::data::AktorData;
