#[doc(hidden)]
pub mod output;
#[doc(hidden)]
pub use output::AktorJobOutput;
pub(crate) use output::{
    AktorCleanupOutput, AktorShutdownAsync, AktorShutdownFuture, AktorShutdownOutput,
    AktorShutdownSync,
};

use alloc::{boxed::Box, string::String, vec::Vec};
use er::*;

/// What shutdown should do with an admitted job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorJob {
    FinishBeforeEnd,
    DropAtStop,
}

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

/// Write this once next to the worker's state. The page and worker use the same contract,
/// so sending the wrong config or returning the wrong ready payload is a compile error.
pub trait AktorWorkerContract: 'static {
    type State: 'static;
    type Role: 'static;
    type Config: AktorData + 'static;
    type Ready: AktorData + 'static;
    type Error: AktorData + 'static;
}

#[doc(hidden)]
pub type AktorNoRole = ();

#[doc(hidden)]
#[allow(non_upper_case_globals)]
pub const AktorNoRole: AktorNoRole = ();

/// A failed shutdown. `report.into_result()` returns this error with the report text.
#[derive(Clone, Er)]
#[er(format = "actor group failed")]
#[non_exhaustive]
pub struct AktorShutdownError {
    pub report: Box<AktorShutdownReport>,
}

/// Retirement could not be confirmed. A stuck actor still owns its state, so anything inside it
/// stays held by that actor.
#[derive(Clone, Er)]
#[er(format = "actor retirement could not be confirmed")]
#[non_exhaustive]
pub struct AktorRetirementError {
    /// The last report, when shutdown published one.
    pub report: Option<Box<AktorShutdownReport>>,
}

/// Confirmed retirement with the final group report or the worker’s lifecycle result.
#[derive(Clone, Debug)]
#[must_use]
pub enum AktorRetired<R = AktorShutdownReport> {
    Released(R),
    Terminated(R),
}

impl<R> AktorRetired<R> {
    pub fn report(&self) -> &R {
        match self {
            Self::Released(report) | Self::Terminated(report) => report,
        }
    }

    pub fn into_report(self) -> R {
        match self {
            Self::Released(report) | Self::Terminated(report) => report,
        }
    }
}

#[derive(Clone, Debug)]
#[must_use]
pub enum AktorRetiredStatus<R = AktorShutdownReport> {
    Pending,
    Confirmed(AktorRetired<R>),
    /// The supervisor has no further evidence. A lost publisher may have left no report.
    Unconfirmed(Option<R>),
}

/// Why the group said no. `Stopping` and `Stopped` are normal during shutdown.
/// The other variants tell you why setup failed.
#[derive(Clone, Er)]

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

#[derive(Clone, Copy, Er)]
#[er(format = "actor cleanup failed")]
pub struct AktorCleanupError;

#[derive(Er)]
#[er(format = "an interval must be greater than zero")]
pub struct AktorIntervalError;

/// Setup and cleanup for an actor owned by the application.
#[doc(hidden)]
pub struct AktorArgs<Setup, Cleanup> {
    pub name: String,
    pub capacity: core::num::NonZeroUsize,
    pub setup: Setup,
    pub cleanup: Cleanup,
}

/// What the supervisor does if work outlives shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorGroupStuck<Abort = ()> {
    KeepRunning,
    AbortProcess(Abort),
}

/// What failed and how cleanup went.
#[derive(Clone, Debug)]
#[must_use]
pub struct AktorShutdownReport {
    pub stop_cause: StopCause,
    pub failures: Vec<AktorFailure>,
    pub actors: Vec<AktorOutcome>,
    pub late_work: Vec<AktorLateWork>,
}

#[derive(Clone, Copy, PartialEq, Eq, ErFormat)]
pub enum AktorLateWork {
    #[er(format = "application work ran past its grace")]
    Application,
    #[er(format = "the group driver ran past its grace")]
    GroupDriver,
    #[er(format = "the shutdown callback ran past its grace")]
    ShutdownCallback,
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
#[derive(Default)]
pub(crate) struct Recorder {
    pub stop_cause: Option<StopCause>,
    pub failures: Vec<AktorFailure>,
    pub actors: Vec<AktorOutcome>,
    pub scopes: Vec<LaunchScope>,
    pub late_work: Vec<AktorLateWork>,
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub(crate) struct LaunchScope {
    pub root: ActorId,
    pub parent: Option<ActorId>,
    pub id: ActorId,
}

#[derive(Clone, Debug)]
pub struct AktorActorInit {
    pub id: ActorId,
    pub name: String,
    pub kind: AktorExecution,
    pub init: AktorInitEvidence,
}

/// Initialization evidence for the actors in a single start or spawn.
#[derive(Clone, Debug)]
pub struct AktorStartup {
    pub actors: Vec<AktorActorInit>,
}

#[derive(Clone, Copy, PartialEq, Eq, Er)]
#[er(display = "the group closed before publishing its shutdown report")]
pub struct AktorPublicationLost;

/// Identifies one failure, including when its report crosses a process boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FailureId {
    producer: [u8; 16],
    counter: core::num::NonZeroU64,
}

impl crate::data::AktorData for FailureId {
    const MEMORY: usize = 0;

    fn encode_data(&self, writer: &mut crate::data::Writer) -> Result<(), crate::data::DataError> {
        writer.write(&self.producer)?;
        writer.write(&self.counter.get())
    }

    fn decode_data(reader: &mut crate::data::Reader<'_>) -> Result<Self, crate::data::DataError> {
        let producer = reader.read()?;
        if producer == [0; 16] {
            return Err(crate::data::DataReason::ZeroFailureId.into());
        }
        core::num::NonZeroU64::new(reader.read()?)
            .map(|counter| Self { producer, counter })
            .ok_or_else(|| crate::data::DataReason::ZeroFailureId.into())
    }
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
static PRODUCER: portable_atomic::AtomicU128 = portable_atomic::AtomicU128::new(0);

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl FailureId {
    /// Supply a fresh random identity at boot on hosts without a system random source.
    #[cfg(any(
        target_os = "none",
        all(
            target_family = "wasm",
            target_os = "unknown",
            not(any(
                feature = "browser_local",
                feature = "wasm_browser_workers",
                feature = "tokio"
            ))
        )
    ))]
    pub fn set_producer(producer: core::num::NonZeroU128) -> Result<(), core::num::NonZeroU128> {
        PRODUCER
            .compare_exchange(
                0,
                producer.get(),
                portable_atomic::Ordering::AcqRel,
                portable_atomic::Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| producer)
    }

    fn producer() -> u128 {
        use portable_atomic::Ordering;
        let existing = PRODUCER.load(Ordering::Acquire);

        #[cfg(any(
            target_os = "none",
            all(
                target_family = "wasm",
                target_os = "unknown",
                not(any(
                    feature = "browser_local",
                    feature = "wasm_browser_workers",
                    feature = "tokio"
                ))
            )
        ))]
        {
            assert!(
                existing != 0,
                "set FailureId::set_producer with a random boot identity first"
            );
            existing
        }
        #[cfg(not(any(
            target_os = "none",
            all(
                target_family = "wasm",
                target_os = "unknown",
                not(any(
                    feature = "browser_local",
                    feature = "wasm_browser_workers",
                    feature = "tokio"
                ))
            )
        )))]
        {
            #[cfg(unix)]
            let process = rustix::process::getpid().as_raw_nonzero().get() as u32;
            if existing != 0 {
                #[cfg(unix)]
                if existing as u32 == process {
                    return existing;
                }
                #[cfg(not(unix))]
                return existing;
            }

            let candidate = loop {
                let mut bytes = [0; 16];
                getrandom::fill(&mut bytes).expect("system entropy for failure IDs");
                // A fork inherits both atomics. Give the child its own identity before its first failure.
                #[cfg(unix)]
                bytes[..4].copy_from_slice(&process.to_le_bytes());
                let value = u128::from_le_bytes(bytes);
                if value != 0 {
                    break value;
                }
            };
            match PRODUCER.compare_exchange(
                existing,
                candidate,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => candidate,
                Err(winner) => winner,
            }
        }
    }

    pub(crate) fn next() -> Self {
        use portable_atomic::{AtomicU64, Ordering};

        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("failure IDs exhausted");
        let producer = Self::producer().to_le_bytes();
        Self {
            producer,
            counter: core::num::NonZeroU64::new(id).expect("failure IDs start at one"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId(pub(crate) usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopCause {
    Requested,
    Retired(ActorId),
    Failure(FailureId),
    StartupFailure(FailureId),
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
#[derive(Clone)]
pub(crate) struct CallLoss(pub crate::AktorDeliveryOutcome, pub Option<Delivery>);
#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl CallLoss {
    pub fn error(self, receive: bool) -> crate::AktorDeliveryError {
        if receive {
            Delivery::error(self.0, self.1.as_ref())
        } else {
            Delivery::reported(self.0, self.1.as_ref())
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Delivery(
    pub(crate) FailureId,
    portable_atomic_util::Arc<DeliveryState>,
);

#[derive(Debug)]
struct DeliveryState {
    received: portable_atomic::AtomicBool,
    #[cfg(any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    report: String,
}

impl Delivery {
    #[cfg(any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub(crate) fn new(id: FailureId, report: String) -> Self {
        Self(
            id,
            portable_atomic_util::Arc::new(DeliveryState {
                received: Default::default(),
                report,
            }),
        )
    }

    #[cfg(any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub(crate) fn report(&self) -> &str {
        &self.1.report
    }

    #[cfg(any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub(crate) fn receive(&self) {
        self.1
            .received
            .store(true, core::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub(crate) fn error(
        outcome: crate::AktorDeliveryOutcome,
        receipt: Option<&Self>,
    ) -> crate::AktorDeliveryError {
        if let Some(receipt) = receipt {
            receipt.receive();
        }
        Self::reported(outcome, receipt)
    }

    #[cfg(any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub(crate) fn reported(
        outcome: crate::AktorDeliveryOutcome,
        receipt: Option<&Self>,
    ) -> crate::AktorDeliveryError {
        match receipt {
            Some(receipt) => crate::AktorDeliveryError {
                outcome,
                report: receipt.report().into(),
                failure_id: Some(receipt.0),
            },
            None => outcome.into(),
        }
    }

    pub(crate) fn received(&self) -> bool {
        self.1.received.load(core::sync::atomic::Ordering::Relaxed)
    }
}

impl PartialEq for Delivery {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Delivery {}

#[doc(hidden)]
pub struct AktorFailureInput {
    pub source: AktorFailureSource<()>,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AktorFailure {
    pub id: FailureId,
    pub(crate) delivery: Delivery,
    pub source: AktorFailureSource,
    pub error: String,
}

#[derive(Clone, ErFormat, PartialEq, Eq)]
pub enum AktorFailureSource<Id = ActorId> {
    #[er(format = "{name:?} ({kind:?}) failed during {phase}")]
    Actor {
        #[er(skip)]
        id: Id,
        name: String,
        kind: AktorExecution,
        phase: AktorPhase,
    },
    #[er(format = "the application task failed")]
    ApplicationTask,
    #[er(format = "the group driver failed")]
    GroupDriver,
    #[er(format = "the shutdown callback failed")]
    ShutdownCallback,
}

#[derive(Clone, ErFormat, PartialEq, Eq)]
pub enum AktorPhase {
    #[er(format = "start")]
    Start,
    #[er(format = "{name}", display = "{name} @ {caller}")]
    Call {
        name: alloc::borrow::Cow<'static, str>,
        caller: Option<alloc::borrow::Cow<'static, str>>,
    },
    #[er(format = "interval")]
    Interval,
    #[er(format = "loop")]
    Loop,
    #[er(format = "background work")]
    Background,
    #[er(format = "runner")]
    Runner,
    #[er(format = "end")]
    End,
    #[er(format = "drop")]
    Drop,
    #[er(format = "join")]
    Join,
    #[er(format = "{0}")]
    Worker(AktorWorkerPhase),
}

#[derive(Clone, Copy, ErFormat, PartialEq, Eq)]
pub enum AktorWorkerPhase {
    #[er(format = "worker setup")]
    Setup,
    #[er(format = "worker transport")]
    Transport,
    #[er(format = "external join")]
    ExternalJoin,
    #[er(format = "worker cleanup")]
    Cleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorInitEvidence {
    NeverEntered,
    Entered,
    Ready,
    Unknown,
}

/// Whether an actor or group has stopped and let go of its resources.
#[derive(Clone, Copy, ErFormat, PartialEq, Eq)]
pub enum AktorRetirement {
    /// Stopped cleanly (`end` returned). Check `report.failed()` for cleanup errors.
    #[er(format = "was released")]
    Released,
    /// Stopped without a cleanup receipt (`end` was interrupted or cut).
    #[er(format = "was terminated")]
    Terminated,
    /// Still running or not yet confirmed (may still hold files, locks or devices).
    #[er(format = "may still be running")]
    StillRunning,
}

/// What shutdown does with an actor it can't stop.
///
/// Actors stop in reverse startup order. With `Kill`, shutdown cuts yielding calls at
/// grace and gives cleanup its own grace period. A browser worker can be terminated.
/// With `Leave`, calls keep draining and `end` can finish after the group publishes
/// its snapshot. Wait for retirement before reopening a resource the actor owns.
///
///
/// ```rust,ignore
/// // A voice call is worth more than a clean database close.
/// stuck: AktorStuck::Leave,
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AktorStuck<Leave = ()>(Option<Leave>);

#[allow(non_upper_case_globals)]
impl AktorStuck {
    /// Keep the actor running. The report marks it as `ran_past_grace` with `StillRunning`
    /// retirement. The left actor still owns its state, so a lock or connection inside it
    /// stays held by that actor and reopening the same file fails on its own. Nothing outside
    /// the actor needs holding. Dedicated threads, `TokioTask`, `BevyTask`, browser workers and
    /// browser threads can be left.
    pub const Leave: Self = Self(Some(()));
}

#[allow(non_upper_case_globals)]
impl<Leave> AktorStuck<Leave> {
    /// Terminate a stuck browser worker. On native, a stuck thread can take down the
    /// whole process when the group was started with `AktorGroupStuck::AbortProcess(())`.
    /// Browser threads can cancel work that yields. Blocking code on browser threads,
    /// local actors, Embassy actors and custom runners can keep running.
    pub const Kill: Self = Self(None);
}

impl<Leave> core::fmt::Debug for AktorStuck<Leave> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(if self.0.is_some() { "Leave" } else { "Kill" })
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct OwnerOutcome {
    pub retirement: AktorRetirement,
    pub init: AktorInitEvidence,
    pub ran_past_grace: bool,
}

#[derive(Clone, Debug)]
pub struct AktorOutcome {
    pub id: ActorId,
    pub name: String,
    /// None when the launch never acquired an owner.
    pub owner: Option<AktorOwnerOutcome>,
    pub kind: AktorExecution,
    pub init: AktorInitEvidence,
}

#[derive(Clone, Copy, Debug)]
pub struct AktorOwnerOutcome {
    pub retirement: AktorRetirement,
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

    pub(crate) use crate::setup::LaunchError;
    pub use crate::setup::{
        AktorBranch, AktorClosure, AktorInit, AktorKind, AktorStartError, AktorStartReason,
        AktorStartRollback,
    };

    /// Run code on a timer or around every call. Use [`AktorHooks::NONE`] when you don't need hooks.
    pub struct AktorHooks<S: 'static, Kind: AktorMode, Before = (), After = ()> {
        pub intervals: Vec<AktorInterval<S, Kind>>,
        pub before_each: Before,
        pub after_each: After,
    }

    pub struct AktorInterval<S: 'static, Kind: AktorMode> {
        pub(crate) every: Duration,
        pub(crate) run: AktorClosure<Kind::Interval<S>>,
    }

    #[doc(hidden)]
    pub trait EndCallback<End, Signature> {
        type Function;
    }
    // Keep the inferred cleanup signature on the setup itself. A helper can
    // then return the setup without boxing it or spelling out its closures.
    impl<End, Signature> EndCallback<End, Signature> for () {
        type Function = End;
    }

    /// One actor. Every field is required on purpose, there's no default `end` or `stuck` for
    /// you to forget about until shutdown.
    pub struct AktorNew<
        S: 'static,
        Start,
        Kind: AktorMode,
        End = (),
        Signature = (),
        Before = (),
        After = (),
    > {
        pub name: String,
        pub kind: Kind,
        /// Builds the state. Calls queue until it returns.
        pub start: Start,
        /// Gets the state after the mailbox closes. Use `()` to drop it.
        pub end: <() as EndCallback<End, Signature>>::Function,
        pub hooks: AktorHooks<S, Kind, Before, After>,
        /// How many calls can wait in its queue.
        pub capacity: core::num::NonZeroUsize,
        pub stuck: super::AktorStuck<Kind::Leave>,
    }

    pub struct AktorSetup<Setups: crate::AktorStart, Shutdown, ShutdownOutput>
    where
        Shutdown: FnOnce(AktorShutdownReport) -> ShutdownOutput,
    {
        pub actors: Setups,
        pub stuck: super::AktorGroupStuck<<Setups::Group as AktorSetupGroup>::Abort>,
        pub shutdown: Shutdown,
        /// How long each actor gets for its calls, and again for its `end`, one actor at a time.
        pub grace: Duration,
    }

    /// Owns the actors it started, dropping it stops them all in reverse order. The handles are
    /// only references. A call after shutdown returns a delivery error.
    ///
    /// ```rust,ignore
    /// let actors = aktor_start(setup);
    /// actors.ready().await?;
    ///
    /// let count = count_users(actors.handles()).await?;
    /// actors.stop().await.into_result()?;
    /// ```
    #[must_use = "dropping it stops the actors it just started"]
    pub struct AktorStarted<Handles, Group: AktorSetupGroup, E: 'static = core::convert::Infallible> {
        pub(crate) handles: Handles,
        pub(crate) group: Group,
        pub(crate) readiness: Readiness<Group::Ready<E>, E>,
    }

    /// More actors started into a running group with `group.spawn(..)`. The group owns them,
    /// this only carries their handles and how their start went.
    #[must_use = "the handles are the only way to call the actors you just started"]
    pub struct AktorSpawned<Handles, Group: AktorSetupGroup, E: 'static = core::convert::Infallible> {
        pub(crate) handles: Handles,
        pub(crate) group: Group,
        pub(crate) readiness: Readiness<Group::Ready<E>, E>,
    }

    use crate::setup::receipt::Readiness;
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

pub use crate::data::AktorData;
