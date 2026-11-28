#[cfg(feature = "bevy")]
use crate::setup::kind::BevyTask;
#[cfg(feature = "std_thread")]
use crate::setup::kind::StdThread;
#[cfg(feature = "tokio")]
use crate::setup::kind::{TokioTask, TokioThread};
use crate::{
    AktorExecution, AktorFailureInput, AktorGroup, AktorGroupError, AktorHooks, AktorJob,
    AktorKillSwitch, AktorNew, AktorStartReason, AktorStopping, AktorStuck, aktor_retire,
    aktor_stopping,
    panic::panic_tree,
    setup::{AktorLaunch, AktorMode, AktorStart, AktorStartContext},
};
use alloc::string::String;
use core::{convert::Infallible, future::Future, ops::AsyncFnOnce};
use er::*;
use futures_util::FutureExt;
use parking_lot::Mutex;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
};
use tokio::sync::watch;

/// Why a loop actor did not return its normal output.
#[derive(Clone, PartialEq, Eq, Er)]

pub enum AktorLoopEnd<E = Infallible> {
    #[er(format = "actor loop failed")]
    Failed(#[er(skip)] E, #[er(skip)] crate::FailureId),
    #[er(format = "actor loop panicked")]
    Panicked(#[er(skip)] crate::FailureId),
    /// Shutdown cut the loop at its grace (`AktorStuck::Kill`).
    #[er(format = "actor loop was cut at shutdown grace")]
    Cut,
    #[er(format = "actor loop failed to start")]
    StartFailed,
}

/// An actor that is just one loop, no calls. The loop gets the stop signal as its argument and
/// returning stops its group. An `Err` becomes a loop failure in the shutdown report.
///
/// ```rust,ignore
/// use core::convert::Infallible;
///
/// let (started, capture) = aktor_start(AktorSetup {
///     stuck: crate::AktorGroupStuck::KeepRunning,
///     actors: AktorLoop {
///         name: "capture".into(),
///         kind: AktorKind::TokioThread,
///         work: async move |mut stopping| {
///             stopping.stopped().await;
///             Ok::<_, (Infallible, String)>(())
///         },
///         stuck: AktorStuck::Leave,
///     },
///     shutdown: |report| log(report),
///     grace: Duration::from_secs(5),
/// }).into_parts();
///
/// started.stop().await.into_result()?;
/// capture.result().await?;
/// ```
pub struct AktorLoop<Kind: AktorMode, F: AktorLoopWork<E>, E = Infallible> {
    pub name: String,
    pub kind: Kind,
    /// Returns `Result<T, X>` with any `X` that turns into `(E, String)`, like an ErResult.
    pub work: <() as LoopWork<F, E>>::Function,
    /// `Leave` when its tail must never be cut at grace. The group reports at grace and
    /// confirms retirement when the loop returns.
    pub stuck: AktorStuck<Kind::Leave>,
}

/// What an [`AktorLoop`] runs. The bound sits on the struct so `async move |mut stopping|`
/// knows its argument is [`AktorStopping`] without spelling it out. Its error can be anything
/// that turns into `(E, String)`, and E rides along on the struct so the handle knows it.
#[doc(hidden)]
pub trait AktorLoopWork<E>:
    AsyncFnOnce(
    AktorStopping,
) -> Result<<Self as AktorLoopWork<E>>::Ok, <Self as AktorLoopWork<E>>::Fail>
{
    type Ok;
    type Fail: Into<(E, String)>;
}
impl<F, T, X, E> AktorLoopWork<E> for F
where
    F: AsyncFnOnce(AktorStopping) -> Result<T, X>,
    X: Into<(E, String)>,
{
    type Ok = T;
    type Fail = X;
}

/// Keeps E in the type of [`AktorLoop`] without a field for it, `Function` is just `F`.
#[doc(hidden)]
pub trait LoopWork<F, E> {
    type Function;
}
impl<F, E> LoopWork<F, E> for () {
    type Function = F;
}

/// Where the loop's result shows up. Stopping the loop is its group's job.
pub struct AktorLoopHandle<T, E = Infallible> {
    kill: AktorKillSwitch,
    state: Arc<LoopState<T, E>>,
}

struct LoopState<T, E> {
    result: Mutex<Option<LoopResult<T, E>>>,
    failure: OnceLock<crate::Delivery>,
    clone_result: OnceLock<Cloner<LoopResult<T, E>>>,
    done: watch::Sender<bool>,
    /// A refused loop never launched and has no cleanup to wait for.
    launched: bool,
}

type LoopResult<T, E> = Result<T, (AktorLoopEnd<E>, String)>;

type Cloner<R> = fn(&R) -> R;

/// Finishes the loop as cut when its future gets dropped before returning.
struct Unfinished<T, E: 'static>(Arc<LoopState<T, E>>);

// The loop is spawned work in a plain actor and `end` waits for it. The group keeps the actor.
macro_rules! loop_actor {
    ($kind:ident, $spawn:path) => {
        type Group = AktorGroup;
        type Error = Infallible;
        type Handles = AktorLoopHandle<T, E>;

        fn actor_name(&self) -> &str {
            &self.name
        }

        fn closed(_name: &str, group: &AktorGroup) -> Self::Handles {
            let kill = group.killswitch();
            let state = LoopState::new(false);
            if !kill.is_stopping() {
                state.finish(Err(er_new!(
                    AktorLoopEnd::StartFailed,
                    AktorGroupError::NotStarted
                )
                .into()));
            }

            AktorLoopHandle { kill, state }
        }

        fn launch(self, context: AktorStartContext<AktorGroup>) -> AktorLaunch<Self> {
            let state = LoopState::new(true);
            let output = Unfinished(state.clone());
            let kill = context.group.killswitch();
            let work = self.work;
            let name = self.name.clone();
            let group = kill.clone();
            let fail = move |phase, message| {
                let failure =
                    AktorFailureInput::actor(name.clone(), AktorExecution::$kind, phase, message);
                group.record_failure(failure, false, |_| {})
            };

            let (_actor, ready) = AktorNew {
                name: self.name,
                kind: self.kind,
                start: crate::AktorInit::Ready(move || {
                    let _receipt = $spawn(
                        run(async move { work(aktor_stopping()).await }, output, fail),
                        AktorJob::FinishBeforeEnd,
                    );
                    core::future::ready(())
                }),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: core::num::NonZeroUsize::new(32).unwrap(),
                stuck: self.stuck,
            }
            .launch(context)?;

            Ok((AktorLoopHandle { kill, state }, ready))
        }
    };
}

#[cfg(feature = "tokio")]
impl<F, T, X, E> AktorStart for AktorLoop<TokioThread, F, E>
where
    T: Send + 'static,
    E: Send + 'static,
    X: Into<(E, String)> + 'static,
    F: AsyncFnOnce(AktorStopping) -> Result<T, X> + Send + 'static,
{
    loop_actor!(TokioThread, crate::aktor_spawn_local);

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn begin(&self, group: &mut AktorGroup) -> Result<(), (AktorStartReason, String)> {
        group
            .start_threaded()
            .er_with(|error| error.start_reason())?;
        Ok(())
    }
}

#[cfg(feature = "std_thread")]
impl<F, T, X, E> AktorStart for AktorLoop<StdThread, F, E>
where
    T: Send + 'static,
    E: Send + 'static,
    X: Into<(E, String)> + 'static,
    F: AsyncFnOnce(AktorStopping) -> Result<T, X> + Send + 'static,
{
    loop_actor!(StdThread, crate::aktor_spawn_local);

    fn begin(&self, group: &mut AktorGroup) -> Result<(), (AktorStartReason, String)> {
        group
            .start_standard()
            .er_with(|error| error.start_reason())?;
        Ok(())
    }
}

#[cfg(feature = "tokio")]
impl<F, Fut, T, X, E> AktorStart for AktorLoop<TokioTask, F, E>
where
    T: Send + 'static,
    E: Send + 'static,
    X: Into<(E, String)> + Send + 'static,
    F: FnOnce(AktorStopping) -> Fut + AktorLoopWork<E> + Send + 'static,
    Fut: Future<Output = Result<T, X>> + Send + 'static,
{
    loop_actor!(TokioTask, crate::aktor_spawn);

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn begin(&self, group: &mut AktorGroup) -> Result<(), (AktorStartReason, String)> {
        group
            .start_threaded()
            .er_with(|error| error.start_reason())?;
        Ok(())
    }
}

#[cfg(feature = "bevy")]
impl<'pool, F, Fut, T, X, E> AktorStart for AktorLoop<BevyTask<'pool>, F, E>
where
    T: Send + 'static,
    E: Send + 'static,
    X: Into<(E, String)> + Send + 'static,
    F: FnOnce(AktorStopping) -> Fut + AktorLoopWork<E> + Send + 'static,
    Fut: Future<Output = Result<T, X>> + Send + 'static,
{
    loop_actor!(BevyTask, crate::aktor_spawn);

    fn begin(&self, group: &mut AktorGroup) -> Result<(), (AktorStartReason, String)> {
        group
            .start_standard()
            .er_with(|error| error.start_reason())?;
        Ok(())
    }
}

#[derive(Er)]
#[er(format = "actor loop failed")]
struct LoopFailure;
async fn run<T, X, E>(
    work: impl Future<Output = Result<T, X>>,
    unfinished: Unfinished<T, E>,
    fail: impl Fn(crate::AktorPhase, String) -> crate::Delivery,
) where
    X: Into<(E, String)>,
    E: 'static,
{
    let ran = AssertUnwindSafe(work).catch_unwind().await;

    match ran {
        Ok(Ok(value)) => {
            unfinished.0.finish(Ok(value));
        }
        Ok(Err(error)) => {
            let (data, report) = error.into();
            let report = er_new!(LoopFailure.er_save("loop", report)).er_report();

            let receipt = fail(crate::AktorPhase::Loop, report.clone());
            let id = receipt.0;
            let _ = unfinished.0.failure.set(receipt);
            unfinished
                .0
                .finish(Err((AktorLoopEnd::Failed(data, id), report)));
        }
        Err(payload) => {
            let report = panic_tree(&payload).er_report();
            let receipt = fail(crate::AktorPhase::Loop, report.clone());
            let id = receipt.0;
            let _ = unfinished.0.failure.set(receipt);
            unfinished
                .0
                .finish(Err((AktorLoopEnd::Panicked(id), report)));
            crate::panic::dispose_with(payload, |nested| {
                fail(crate::AktorPhase::Drop, panic_tree(nested).er_report());
            });
        }
    };

    aktor_retire();
}

impl<T, E: 'static> AktorLoopHandle<T, E> {
    pub fn is_finished(&self) -> bool {
        *self.state.done.borrow()
    }

    /// Take this handle's result. When racing a timeout, pin the future and poll it by
    /// reference to keep your reader if the timeout wins.
    /// The registered launch attempt this handle belongs to.
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        self.kill.registered_id()
    }

    pub async fn result(self) -> Result<T, (AktorLoopEnd<E>, String)> {
        self.state.wait(&self.kill).await;
        let mut result = self.state.result.lock();
        let outcome = match (self.state.clone_result.get(), result.as_ref()) {
            (Some(clone), Some(result)) => clone(result),
            _ => match result.take() {
                Some(result) => result,
                None => er_bail!(AktorLoopEnd::Cut),
            },
        };

        if outcome.is_err()
            && let Some(delivery) = self.state.failure.get()
        {
            delivery.receive();
        }
        outcome
    }
}

impl<T: Clone, E: Clone> Clone for AktorLoopHandle<T, E> {
    fn clone(&self) -> Self {
        self.state.clone_result.get_or_init(|| Clone::clone);

        Self {
            kill: self.kill.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T, E: 'static> LoopState<T, E> {
    fn new(launched: bool) -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            failure: OnceLock::new(),
            clone_result: OnceLock::new(),
            done: watch::channel(false).0,
            launched,
        })
    }

    fn finish(&self, result: LoopResult<T, E>) {
        let mut slot = self.result.lock();
        if slot.is_some() || *self.done.borrow() {
            return;
        }

        *slot = Some(result);
        drop(slot);

        self.done.send_replace(true);
    }

    async fn wait(&self, kill: &AktorKillSwitch) {
        if self.launched {
            let mut done = self.done.subscribe();
            let _ = done.wait_for(|done| *done).await;
        } else if !*self.done.borrow() {
            let report = kill.completion().wait().await;
            self.finish(Err(er_new!(AktorLoopEnd::StartFailed.er_save(
                "shutdown",
                || {
                    report
                        .as_ref()
                        .map(|report| report.render(None))
                        .unwrap_or_default()
                }
            ))
            .into()));
        }
    }
}

impl<T, E: 'static> Drop for Unfinished<T, E> {
    fn drop(&mut self) {
        if !*self.0.done.borrow() {
            self.0.finish(Err(er_new!(AktorLoopEnd::Cut).into()));
        }
    }
}
