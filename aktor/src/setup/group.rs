use super::receipt::Stage;
use super::*;
#[cfg(any(target_family = "wasm", feature = "local"))]
use crate::message::LocalFuture;
use crate::{AktorShutdownReport, AktorSpawned};
use core::task::Poll;

#[doc(hidden)]
pub trait AktorSetupGroup: Sized + Unpin {
    type AktorKillSwitch: Clone + Unpin;
    type Completion: Clone
        + Unpin
        + core::future::IntoFuture<Output = Result<AktorShutdownReport, crate::AktorPublicationLost>>;
    /// Finishes once `start` returned. Native groups keep it `Send` so the started value can
    /// move between tasks.
    type Ready<E: 'static>: Future<Output = Result<(), StartFailure<E>>> + Unpin;

    type Abort: Copy;
    fn new(grace: Duration, stuck: crate::AktorGroupStuck<Self::Abort>) -> Self;
    fn startup_receipt(&self) -> crate::AktorStartup;
    fn registration(&self) -> Self;
    fn track_startup(&mut self) {}
    fn fallback_member(&self, index: Option<usize>) -> Self;
    fn track_member(&mut self) {}
    fn validate_attempt(&self, name: &str, kind: AktorExecution);
    fn startup_id(kill: &Self::AktorKillSwitch) -> Option<crate::FailureId>;
    fn source_is_failure(_kill: &Self::AktorKillSwitch) -> bool {
        false
    }
    fn check_started(&self) -> Result<(), (AktorGroupError, String)>;
    fn killswitch(&self) -> Self::AktorKillSwitch;
    fn completion(&self) -> Self::Completion;
    fn stop(kill: &Self::AktorKillSwitch);
    fn fail_startup(
        kill: &Self::AktorKillSwitch,
        failure: crate::AktorFailureInput,
    ) -> crate::FailureId;

    /// Stopped from outside or failed while a `start` was still running.
    fn startup_aborted(kill: &Self::AktorKillSwitch) -> bool;

    fn observe_ready<E: 'static>(
        ready: Self::Ready<E>,
        kill: Self::AktorKillSwitch,
        name: String,
    ) -> Self::Ready<E>;
    fn failed_ready<E: 'static>(
        reason: AktorStartReason,
        failure_id: Option<crate::FailureId>,
        text: String,
    ) -> Self::Ready<E>;

    /// Ready once both are, the first failure wins.
    fn pair<A: 'static, B: 'static>(
        first: Self::Ready<A>,
        second: Self::Ready<B>,
        first_kill: Self::AktorKillSwitch,
        second_kill: Self::AktorKillSwitch,
    ) -> Self::Ready<AktorBranch<A, B>>;

    fn map_ready<A: 'static, B: 'static>(
        ready: Self::Ready<A>,
        map: impl FnOnce(LaunchError<A>) -> LaunchError<B> + Send + 'static,
    ) -> Self::Ready<B>;
}

pub(super) fn identify<G: AktorSetupGroup, E>(
    kill: &G::AktorKillSwitch,
    name: String,
    failure: &mut StartFailure<E>,
) {
    if failure.0.failure_id.is_none() {
        failure.0.failure_id = G::startup_id(kill);
    }
    if failure.0.reason != AktorStartReason::Stopping && failure.0.failure_id.is_none() {
        failure.0.failure_id = Some(G::fail_startup(
            kill,
            crate::AktorFailureInput::actor(
                name,
                AktorExecution::Custom,
                crate::AktorPhase::Start,
                failure.1.clone(),
            ),
        ));
    }
}

async fn observed<G: AktorSetupGroup, E: 'static>(
    ready: G::Ready<E>,
    kill: G::AktorKillSwitch,
    name: String,
) -> Result<(), StartFailure<E>> {
    if let Err(mut failure) = ready.await {
        identify::<G, E>(&kill, name, &mut failure);
        return Err(failure);
    }

    Ok(())
}

/// Waits for both. A failure with your own error in `init` is the reason and wins right away,
/// the other one usually just got stopped along with it.
async fn both<A, B>(
    mut first: impl Future<Output = Result<(), StartFailure<A>>> + Unpin,
    mut second: impl Future<Output = Result<(), StartFailure<B>>> + Unpin,
    first_primary: impl Fn() -> bool,
    second_primary: impl Fn() -> bool,
) -> Result<(), StartFailure<AktorBranch<A, B>>> {
    // Only failures without your error wait here, so nothing of `A` or `B` is held across a wait.
    let mut first_done: Option<Result<(), StartFailure>> = None;
    let mut second_done: Option<Result<(), StartFailure>> = None;

    let decided = core::future::poll_fn(|cx| {
        if first_done.is_none()
            && !second_primary()
            && let Poll::Ready(result) = Pin::new(&mut first).poll(cx)
        {
            match result {
                Ok(()) => first_done = Some(Ok(())),
                Err((error, text))
                    if first_primary() || (error.init.is_some() && !second_primary()) =>
                {
                    return Poll::Ready(Some((error.map_init(AktorBranch::First), text)));
                }
                Err((error, text)) => {
                    first_done = Some(Err((
                        LaunchError {
                            init: None,

                            reason: error.reason,
                            failure_id: error.failure_id,
                        },
                        text,
                    )));
                }
            }
        }

        if second_done.is_none()
            && !first_primary()
            && let Poll::Ready(result) = Pin::new(&mut second).poll(cx)
        {
            match result {
                Ok(()) => second_done = Some(Ok(())),
                Err((error, text))
                    if second_primary() || (error.init.is_some() && !first_primary()) =>
                {
                    return Poll::Ready(Some((error.map_init(AktorBranch::Second), text)));
                }
                Err((error, text)) => {
                    second_done = Some(Err((
                        LaunchError {
                            init: None,

                            reason: error.reason,
                            failure_id: error.failure_id,
                        },
                        text,
                    )));
                }
            }
        }

        match (&first_done, &second_done) {
            (Some(_), Some(_)) => Poll::Ready(None),
            (None, _) | (_, None) => Poll::Pending,
        }
    })
    .await;

    if let Some(error) = decided {
        return Err(error);
    }

    match (first_done, second_done) {
        (Some(Err((error, text))), _) | (_, Some(Err((error, text)))) => {
            Err((error.map_init(|never| match never {}), text))
        }
        (Some(Ok(())) | None, Some(Ok(())) | None) => Ok(()),
    }
}

/// Starts more actors in a group that is already running, for an actor that needs another one's
/// handle to start. It's owned by the group like the rest, and stops in reverse order with them.
fn spawn_in<Setup: AktorStart>(
    group: &Setup::Group,
    setup: Setup,
) -> AktorSpawned<Setup::Handles, Setup::Group, Setup::Error> {
    let name = String::from(setup.actor_name());
    let fallback = setup.fallback();

    // A group that isn't running refuses the actor and stays as it was.
    if let Err(error) = group.check_started() {
        return AktorSpawned {
            handles: fallback(&group.fallback_member(None)),
            group: group.registration(),
            readiness: super::receipt::Readiness::refused(
                er_new!(
                    AktorStartError::Refused {
                        reason: error.0.start_reason()
                    },
                    error
                )
                .into(),
            ),
        };
    }

    // Its own startup tracking, so the report can tell this actor's failure is the reason.
    let mut joined = group.registration();
    joined.track_startup();

    let kill = joined.killswitch();
    let launched = setup.launch(AktorStartContext {
        group: joined.registration(),
    });

    match launched {
        Ok((handles, ready)) => AktorSpawned {
            handles,
            group: joined,
            readiness: Stage::Starting(Setup::Group::observe_ready(ready, kill, name)).into(),
        },
        Err(mut error) => {
            identify::<Setup::Group, _>(&kill, name, &mut error);
            Setup::Group::stop(&kill);

            AktorSpawned {
                handles: fallback(&joined),
                group: joined,
                readiness: Stage::Failed(error).into(),
            }
        }
    }
}

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
impl crate::AktorGroup {
    /// Start more actors in this group, the same way `aktor_start` does. A failed start stops the
    /// whole group.
    ///
    /// ```rust,ignore
    /// let bodies = started.group().spawn(bodies_setup).ready().await?;
    /// ```
    pub fn spawn<Setup>(&self, setup: Setup) -> AktorSpawned<Setup::Handles, Self, Setup::Error>
    where
        Setup: AktorStart<Group = Self>,
    {
        spawn_in(self, setup)
    }
}

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
impl AktorSetupGroup for crate::AktorGroup {
    type AktorKillSwitch = crate::AktorKillSwitch;
    type Completion = crate::AktorGroupCompletion;
    #[cfg(not(target_family = "wasm"))]
    type Ready<E: 'static> = AktorThreadFuture<Result<(), StartFailure<E>>>;
    #[cfg(target_family = "wasm")]
    type Ready<E: 'static> = LocalFuture<'static, Result<(), StartFailure<E>>>;

    #[cfg(not(target_family = "wasm"))]
    type Abort = ();
    #[cfg(target_family = "wasm")]
    type Abort = core::convert::Infallible;

    fn new(grace: Duration, stuck: crate::AktorGroupStuck<Self::Abort>) -> Self {
        #[cfg(not(target_family = "wasm"))]
        return Self::new_group(grace, stuck);
        #[cfg(target_family = "wasm")]
        {
            let _ = stuck;
            Self::with_grace(grace)
        }
    }

    fn startup_receipt(&self) -> crate::AktorStartup {
        self.startup_receipt()
    }

    fn registration(&self) -> Self {
        self.new_registration()
    }

    fn track_startup(&mut self) {
        self.track_startup();
    }

    fn fallback_member(&self, index: Option<usize>) -> Self {
        self.fallback_member(index)
    }

    fn track_member(&mut self) {
        self.track_member();
    }

    fn observe_ready<E: 'static>(
        ready: Self::Ready<E>,
        kill: Self::AktorKillSwitch,
        name: String,
    ) -> Self::Ready<E> {
        Box::pin(observed::<Self, E>(ready, kill, name))
    }

    fn failed_ready<E: 'static>(
        reason: AktorStartReason,
        failure_id: Option<crate::FailureId>,
        text: String,
    ) -> Self::Ready<E> {
        Box::pin(async move {
            Err((
                LaunchError {
                    failure_id,
                    ..LaunchError::new(reason)
                },
                text,
            ))
        })
    }

    fn validate_attempt(&self, name: &str, kind: AktorExecution) {
        self.killswitch().validate_attempt(name, kind);
    }

    fn startup_id(kill: &Self::AktorKillSwitch) -> Option<crate::FailureId> {
        kill.startup_id()
    }

    fn source_is_failure(kill: &Self::AktorKillSwitch) -> bool {
        kill.has_startup_failure()
    }

    fn check_started(&self) -> Result<(), (AktorGroupError, String)> {
        self.check_started()
    }

    fn killswitch(&self) -> Self::AktorKillSwitch {
        self.killswitch()
    }

    fn completion(&self) -> Self::Completion {
        self.completion()
    }

    fn stop(kill: &Self::AktorKillSwitch) {
        kill.stop();
    }

    fn fail_startup(
        kill: &Self::AktorKillSwitch,
        failure: crate::AktorFailureInput,
    ) -> crate::FailureId {
        kill.fail_startup(failure)
    }

    fn startup_aborted(kill: &Self::AktorKillSwitch) -> bool {
        kill.is_startup_aborted()
    }

    fn pair<A: 'static, B: 'static>(
        first: Self::Ready<A>,
        second: Self::Ready<B>,
        first_kill: Self::AktorKillSwitch,
        second_kill: Self::AktorKillSwitch,
    ) -> Self::Ready<AktorBranch<A, B>> {
        Box::pin(both(
            first,
            second,
            move || Self::source_is_failure(&first_kill),
            move || Self::source_is_failure(&second_kill),
        ))
    }

    fn map_ready<A: 'static, B: 'static>(
        ready: Self::Ready<A>,
        map: impl FnOnce(LaunchError<A>) -> LaunchError<B> + Send + 'static,
    ) -> Self::Ready<B> {
        Box::pin(async move {
            match ready.await {
                Ok(()) => Ok(()),
                Err((error, text)) => Err((map(error), text)),
            }
        })
    }
}

#[cfg(feature = "local")]
impl<Clock: crate::local::clock::AktorGroupClock> crate::local::AktorGroup<Clock> {
    /// Start more actors in this group, the same way `aktor_start` does. A failed start stops the
    /// whole group.
    pub fn spawn<Setup>(&self, setup: Setup) -> AktorSpawned<Setup::Handles, Self, Setup::Error>
    where
        Setup: AktorStart<Group = Self>,
    {
        spawn_in(self, setup)
    }
}

#[cfg(feature = "local")]
impl<Clock: crate::local::clock::AktorGroupClock> AktorSetupGroup
    for crate::local::AktorGroup<Clock>
{
    type AktorKillSwitch = crate::local::AktorKillSwitch<Clock>;
    type Completion = crate::local::AktorGroupCompletion<Clock>;
    type Ready<E: 'static> = LocalFuture<'static, Result<(), StartFailure<E>>>;

    type Abort = core::convert::Infallible;

    fn new(grace: Duration, _: crate::AktorGroupStuck<Self::Abort>) -> Self {
        Self::with_grace(grace)
    }

    fn startup_receipt(&self) -> crate::AktorStartup {
        self.startup_receipt()
    }

    fn registration(&self) -> Self {
        self.new_registration()
    }

    fn track_startup(&mut self) {
        self.track_startup();
    }

    fn fallback_member(&self, index: Option<usize>) -> Self {
        self.fallback_member(index)
    }

    fn track_member(&mut self) {
        self.track_member();
    }

    fn observe_ready<E: 'static>(
        ready: Self::Ready<E>,
        kill: Self::AktorKillSwitch,
        name: String,
    ) -> Self::Ready<E> {
        Box::pin(observed::<Self, E>(ready, kill, name))
    }

    fn failed_ready<E: 'static>(
        reason: AktorStartReason,
        failure_id: Option<crate::FailureId>,
        text: String,
    ) -> Self::Ready<E> {
        Box::pin(async move {
            Err((
                LaunchError {
                    failure_id,
                    ..LaunchError::new(reason)
                },
                text,
            ))
        })
    }

    fn validate_attempt(&self, name: &str, kind: AktorExecution) {
        self.killswitch().validate_attempt(name, kind);
    }

    fn startup_id(kill: &Self::AktorKillSwitch) -> Option<crate::FailureId> {
        kill.startup_id()
    }

    fn source_is_failure(kill: &Self::AktorKillSwitch) -> bool {
        kill.has_startup_failure()
    }

    fn check_started(&self) -> Result<(), (AktorGroupError, String)> {
        self.check_started()
    }

    fn killswitch(&self) -> Self::AktorKillSwitch {
        self.killswitch()
    }

    fn completion(&self) -> Self::Completion {
        self.completion()
    }

    fn stop(kill: &Self::AktorKillSwitch) {
        kill.stop();
    }

    fn fail_startup(
        kill: &Self::AktorKillSwitch,
        failure: crate::AktorFailureInput,
    ) -> crate::FailureId {
        kill.fail_startup(failure)
    }

    fn startup_aborted(kill: &Self::AktorKillSwitch) -> bool {
        kill.is_startup_aborted()
    }

    fn pair<A: 'static, B: 'static>(
        first: Self::Ready<A>,
        second: Self::Ready<B>,
        first_kill: Self::AktorKillSwitch,
        second_kill: Self::AktorKillSwitch,
    ) -> Self::Ready<AktorBranch<A, B>> {
        Box::pin(both(
            first,
            second,
            move || Self::source_is_failure(&first_kill),
            move || Self::source_is_failure(&second_kill),
        ))
    }

    fn map_ready<A: 'static, B: 'static>(
        ready: Self::Ready<A>,
        map: impl FnOnce(LaunchError<A>) -> LaunchError<B> + Send + 'static,
    ) -> Self::Ready<B> {
        Box::pin(async move {
            match ready.await {
                Ok(()) => Ok(()),
                Err((error, text)) => Err((map(error), text)),
            }
        })
    }
}
