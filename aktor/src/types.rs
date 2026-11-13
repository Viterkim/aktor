#[doc(hidden)]
pub mod output;
pub(crate) use output::{
    AktorCleanupOutput, AktorShutdownAsync, AktorShutdownFuture, AktorShutdownOutput,
    AktorShutdownSync,
};

use alloc::{boxed::Box, string::String, vec::Vec};

/// The execution kind that appears in reports. Use `AktorKind` to choose it during setup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

#[doc(hidden)]
pub type AktorNoRole = ();

#[doc(hidden)]
#[allow(non_upper_case_globals)]
pub const AktorNoRole: AktorNoRole = ();

/// A failed shutdown. `report.into_result()` returns this error with the report text.
#[derive(Clone, er::Er)]
#[er(no_constructors, format = "actor group failed")]
#[non_exhaustive]
pub struct AktorShutdownError {
    pub report: Box<AktorShutdownReport>,
}

/// Retirement could not be confirmed. Keep resources reserved until their owner lets go.
#[derive(Clone, er::Er)]
#[er(no_constructors, format = "actor retirement could not be confirmed")]
#[non_exhaustive]
pub struct AktorRetirementError {
    /// The last report, when shutdown published one.
    pub report: Option<Box<AktorShutdownReport>>,
}

/// Why the group said no. `Stopping` and `Stopped` are normal during shutdown.
/// The other variants tell you why setup failed.
#[derive(Clone, er::Er)]
#[er(no_constructors)]
pub enum AktorGroupError {
    #[er(format = "start the actor group first")]
    NotStarted,
    #[er(format = "actor group is stopping")]
    Stopping,
    #[er(format = "actor group already stopped")]
    Stopped,
    #[er(format = "actor group already has a shutdown closure")]
    ShutdownTaken,
    #[er(format = "actor group already has a shutdown listener")]
    ListenerTaken,
    #[er(format = "needs a Tokio runtime")]
    NoTokio,
    #[er(format = "use AktorKind::StdThread or enable the tokio feature")]
    NoDefaultRuntime,
    #[er(format = "actor group failed to start")]
    StartFailed { report: Box<AktorShutdownReport> },
}

/// A setup or cleanup diagnostic in the shutdown report.
#[cfg_attr(feature = "transport", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, PartialEq, Eq, er::Er)]
#[er(display = Self::fmt_display, display_alt = "{diagnostics}", debug = Self::fmt_display)]
pub struct AktorError<T = ()> {
    pub diagnostics: String,
    #[er(default)]
    pub data: T,
}

#[doc(hidden)]
#[derive(Clone, er::Er)]
#[er(
    no_constructors,
    display = Self::fmt_display,
    display_alt = "{diagnostics}",
    debug = Self::fmt_display
)]
pub struct AktorFailure {
    pub diagnostics: String,
    #[er(skip)]
    pub primary: bool,
}

/// Setup and cleanup for an actor owned by the application.
#[doc(hidden)]
pub struct AktorArgs<Setup, Cleanup> {
    pub name: String,
    pub capacity: usize,
    pub setup: Setup,
    pub cleanup: Cleanup,
}

/// What a native group does when shutdown gets stuck.
#[cfg(all(
    any(
        feature = "tokio",
        feature = "std_thread",
        feature = "wasm_browser_workers"
    ),
    not(target_family = "wasm")
))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorShutdownPolicy {
    /// Leave the process running. This is the default.
    KeepAlive,
    /// Terminate the process if shutdown gets stuck or Aktor can't start its watchdog.
    AbortProcess,
}

/// What failed and how cleanup went.
#[derive(Clone, Debug, Default)]
pub struct AktorShutdownReport {
    /// Stopped during root startup, or because a new actor could not initialize.
    pub startup: bool,
    pub group_error: Option<AktorGroupShutdownError>,
    pub failure: Option<AktorFailureReport>,
    pub actors: Vec<AktorOutcome>,
    pub application: Vec<AktorError>,
    /// Work was still running after its shutdown grace.
    pub ran_past_grace: bool,
}

/// What went wrong with the group's own shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorGroupShutdownError {
    /// The group disappeared before publishing a report.
    ReportLost,
    /// Running or dropping the group's shutdown callback took longer than its grace.
    CallbackTimedOut,
}

/// What a group completion currently knows about its actors' retirement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorGroupRetirement {
    /// The group has yet to publish a shutdown report.
    Pending,
    /// The group disappeared before publishing a shutdown report.
    ReportLost,
    /// A shutdown report is available, with no actor outcomes.
    NoActors,
    /// Every actor's retirement was confirmed. Cleanup may have failed.
    AllConfirmed,
    /// Every actor stopped, and at least one stopped without a cleanup receipt.
    SomeTerminated,
    /// At least one actor may still hold its resources.
    Unconfirmed,
}

#[derive(Clone, Debug)]
pub struct AktorFailureReport {
    pub actor: String,
    pub kind: Option<AktorExecution>,
    pub phase: String,
    pub message: String,
}

/// What the owner has reported about its `end` callback so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorCleanupStatus {
    /// Still waiting for `end`.
    Pending,
    Succeeded,
    /// `end` returned an error. `wait_cleanup()` has its details.
    Failed,
    /// Aktor cannot confirm how `end` finished.
    Unconfirmed,
}

/// Whether the owner has stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorRetireStatus {
    /// The owner has stopped. Its cleanup may have returned an error.
    Confirmed,
    /// The owner may still hold its resources. Keep them reserved.
    Unconfirmed,
    /// The owner has stopped without a cleanup receipt. Its `end` may have been interrupted.
    Terminated,
}

/// What shutdown does with an actor it can't stop.
///
/// Shutdown stops actors one at a time in reverse startup order. Aktor gives each actor
/// `grace` to finish its calls, then cancels calls that can yield. It gives `end`
/// another `grace` and cancels it if it can yield. Browser workers get both periods at
/// once. Browser threads with `Leave` keep draining their calls when the first period ends.
///
/// This setting decides what happens when shutdown can't stop the actor, for example
/// if it's stuck in blocking code. The group moves on either way.
///
/// ```rust,ignore
/// AktorNew {
///     name: "database".into(),
///     // A voice call is worth more than a clean database close.
///     stuck: AktorStuck::Leave,
///     ..
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorStuck {
    /// Keep the actor running. The report marks it as `ran_past_grace` with `Unconfirmed`
    /// retirement. Keep its resources reserved because it may still own them.
    Leave,
    /// Terminate a stuck browser worker. On native, a stuck thread can take down the
    /// whole process when the group was started with `AktorShutdownPolicy::AbortProcess`.
    /// Browser threads can cancel work that yields. Blocking code on browser threads,
    /// local actors, Embassy actors and custom runners can keep running.
    Kill,
}

#[derive(Clone, Debug)]
pub struct AktorOutcome {
    pub retirement: AktorRetireStatus,
    pub actor: String,
    pub kind: AktorExecution,
    pub diagnostics: Vec<AktorError>,
    /// The owner was still running when its shutdown grace expired.
    pub ran_past_grace: bool,
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
    use super::AktorShutdownReport;
    use crate::setup::{AktorMode, group::AktorSetupGroup};
    use alloc::{string::String, vec::Vec};
    use core::time::Duration;

    pub use crate::setup::{
        AktorBranch, AktorClosure, AktorInitOutcome, AktorKind, AktorStartCause, AktorStartError,
        AktorStartup, aktor_ready,
    };

    /// Run code on a timer or around every call. `Default::default()` when you don't need any.
    pub struct AktorHooks<S: 'static, Kind: AktorMode> {
        pub intervals: Vec<AktorInterval<S, Kind>>,
        pub before_each: Option<AktorClosure<Kind::Each<S>>>,
        pub after_each: Option<AktorClosure<Kind::Each<S>>>,
    }

    pub struct AktorInterval<S: 'static, Kind: AktorMode> {
        pub every: Duration,
        pub run: AktorClosure<Kind::Interval<S>>,
    }

    #[doc(hidden)]
    pub trait EndCallback<End, Signature> {
        type Function;
    }
    // Keep the inferred startup and cleanup signature on the setup itself. A helper can
    // then return the setup without boxing it or spelling out its closures.
    impl<End, Signature> EndCallback<End, Signature> for () {
        type Function = End;
    }

    pub struct AktorNew<S: 'static, Start, Kind: AktorMode, End = (), Signature = ()> {
        pub name: String,
        pub kind: Kind,
        pub start: Start,
        pub end: <() as EndCallback<End, Signature>>::Function,
        pub hooks: AktorHooks<S, Kind>,
        pub capacity: usize,
        pub stuck: super::AktorStuck,
    }

    pub struct AktorSetup<Setups, Shutdown, ShutdownOutput>
    where
        Shutdown: FnOnce(AktorShutdownReport) -> ShutdownOutput,
    {
        pub actors: Setups,
        pub shutdown: Shutdown,
        /// How long each actor gets for its calls, and again for its `end`, one actor at a time.
        pub grace: Duration,
    }

    /// Keep this owner alive while using its handles. Dropping its group starts shutdown.
    ///
    /// ```rust,ignore
    /// let actors = aktor_start(setup).await.er(())?;
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
