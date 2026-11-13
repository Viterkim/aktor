use super::closures::AktorEndCall;
use super::*;
use crate::{
    Aktor, AktorArgs, AktorError, AktorFailure,
    listener::{Handle, hooks::CallHooks},
    message::call_async,
    operation::Operation,
};
use er::*;
use parking_lot::Mutex;
use std::{
    ops::AsyncFnOnce,
    sync::{Arc, Weak},
};
use tokio::sync::oneshot;

#[cfg(all(test, feature = "std_thread"))]
mod tests;

#[cfg(feature = "tokio")]
use super::kind::TokioThread;

#[cfg(feature = "tokio")]
impl<S, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, TokioThread, End, (CM, E, M)>
where
    S: 'static,
    End: AktorEndCall<S, CM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, E> + Send + 'static,
    E: InitError<M> + 'static,
    E::Error: Send + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E::Error;
    type Handles = Aktor<S>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start_threaded()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        Box::pin(start_thread(self, context, false))
    }
}
#[cfg(feature = "std_thread")]
impl<S, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, kind::StdThread, End, (CM, E, M)>
where
    S: 'static,
    End: AktorEndCall<S, CM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, E> + Send + 'static,
    E: InitError<M> + 'static,
    E::Error: Send + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E::Error;
    type Handles = Aktor<S>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start_standard()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        Box::pin(start_thread(self, context, true))
    }
}
struct ThreadInterval<S> {
    every: Duration,
    callback: Weak<Mutex<Option<AktorClosure<dyn AktorIntervalLogic<S> + Send>>>>,
}

async fn start_thread<S, Start, Kind, End, CM, E, M>(
    setup: AktorNew<S, Start, Kind, End, (CM, E, M)>,
    mut context: AktorStartContext<crate::AktorGroup>,
    standard: bool,
) -> Result<Aktor<S>, AktorStartError<E::Error>>
where
    S: 'static,
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation) + Send,
            Interval<S> = dyn AktorIntervalLogic<S> + Send,
        >,
    End: AktorEndCall<S, CM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, E> + Send + 'static,
    E: InitError<M> + 'static,
    E::Error: Send + 'static,
{
    let AktorNew {
        name,
        start,
        end,
        hooks,
        capacity,
        stuck,
        ..
    } = setup;
    let AktorHooks {
        intervals,
        before_each,
        after_each,
    } = hooks;

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        return Err(AktorStartError::not_run(validation_error(
            &context.group,
            &name,
            Kind::EXECUTION,
            AktorStartCause::Setup(AktorError::new("interval duration must be positive", ())),
        )));
    }

    if capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(AktorStartError::not_run(validation_error(
            &context.group,
            &name,
            Kind::EXECUTION,
            AktorStartCause::Thread(crate::listener::DedicatedStartError::InvalidCapacity),
        )));
    }

    let hooks = CallHooks {
        before_each: before_each.map(|hook| hook.0),
        after_each: after_each.map(|hook| hook.0),
    };
    let mut retained = Vec::new();
    let intervals: Vec<_> = intervals
        .into_iter()
        .map(|interval| {
            let callback = Arc::new(Mutex::new(Some(interval.run)));

            retained.push(callback.clone());
            ThreadInterval {
                every: interval.every,
                callback: Arc::downgrade(&callback),
            }
        })
        .collect();

    let mut cleanup = Some((end, retained));
    let startup_kill = context.group.killswitch();
    let startup_name = name.clone();
    let (typed, mut initial_error) = oneshot::channel();
    let (enter, mut entered) = oneshot::channel();
    let actor = context
        .group
        .spawn_async_initial_on_reported::<S, _, _, _, _, _>(
            AktorArgs {
                name: name.clone(),
                capacity,
                setup: async move || {
                    let _ = enter.send(());
                    match start().await {
                        Ok(state) => Ok(state),
                        Err(error) => {
                            let (data, report) = error.split();
                            let mut unsent = None;
                            let failure = startup_kill.startup_error_with(
                                startup_name,
                                Kind::EXECUTION,
                                crate::AktorError::new(report, ()),
                                |primary, diagnostics| {
                                    unsent = typed
                                        .send((
                                            data,
                                            AktorFailure {
                                                diagnostics: diagnostics.into(),
                                                primary,
                                            },
                                        ))
                                        .err();
                                },
                            );
                            crate::group::shutdown::contain_drop(
                                unsent,
                                &startup_kill,
                                "startup reply drop",
                            );
                            Err(failure)
                        }
                    }
                },
                cleanup: move |state| {
                    let cleanup = cleanup.take();

                    async move {
                        match cleanup {
                            Some((end, _retained)) => end.run(state).await,
                            None => Ok(()),
                        }
                    }
                },
            },
            hooks,
            standard,
            stuck,
            |failure| (failure.diagnostics.clone(), failure.primary),
        )
        .await
        .map_err(|error| {
            // A live sender can still enter start if the supervisor disappears.
            let mut init = match entered.try_recv() {
                Err(oneshot::error::TryRecvError::Closed) => AktorInitOutcome::NotRun,
                _ => AktorInitOutcome::Unconfirmed,
            };
            let cause = match initial_error.try_recv() {
                Ok((data, failure)) => {
                    init = AktorInitOutcome::Failed(data);
                    if matches!(error.primary(), DedicatedStartError::Closed) {
                        AktorStartCause::Init(failure)
                    } else {
                        error.into()
                    }
                }
                Err(_) => error.into(),
            };
            AktorStartError {
                init,
                cause,
                report: None,
            }
        })?;

    for interval in intervals {
        if let Err(tree) = start_interval(interval, &actor, &context.group, &name, standard) {
            return Err(validation_error(
                &context.group,
                &name,
                Kind::EXECUTION,
                AktorStartCause::Setup(AktorError::new(tree.er_report(), ())),
            )
            .into());
        }
    }

    Ok(actor)
}

#[derive(Er)]
pub struct IntervalErr {
    pub actor: String,
}
fn start_interval<S: 'static>(
    interval: ThreadInterval<S>,
    actor: &Aktor<S>,
    group: &crate::AktorGroup,
    name: &str,
    standard: bool,
) -> ErResult<(), IntervalErr> {
    let spawner = group.spawner(standard).er(name)?;

    schedule(
        interval,
        actor.handle(),
        group.killswitch(),
        standard,
        group,
        spawner,
    )
    .er(name)
}

fn schedule<S: 'static>(
    interval: ThreadInterval<S>,
    handle: Handle<S>,
    kill: crate::AktorKillSwitch,
    standard: bool,
    group: &crate::AktorGroup,
    spawner: crate::executor::Spawner,
) -> std::io::Result<()> {
    let callback = interval.callback;
    let lifetime = kill.track_thread("aktor interval".into(), crate::AktorStuck::Kill);

    let work = async move {
        let _lifetime = lifetime;

        loop {
            tokio::select! {
                biased;
                _ = handle.wait_closing() => break,
                _ = interval_wait(interval.every, standard) => {},
            }

            let callback = callback.clone();
            let request = call_async(
                &handle,
                async move |state: &mut S, ()| {
                    let Some(callback) = callback.upgrade() else {
                        return;
                    };
                    let work = callback.lock().take();

                    if let Some(mut work) = work {
                        work.0.run(state).await;
                        *callback.lock() = Some(work);
                    }
                },
                (),
            )
            .operation(Operation {
                name: "interval",
                caller: std::panic::Location::caller(),
            });

            tokio::select! {
                biased;
                _ = handle.wait_closing() => break,
                admitted = request.run_interval() => {
                    if !admitted { break; }
                },
            }
        }
    };

    #[cfg(feature = "std_thread")]
    if standard {
        return group.spawn_interval(work);
    }

    let _ = group;
    spawner.spawn_named("aktor interval", work)
}

async fn interval_wait(every: Duration, standard: bool) {
    #[cfg(feature = "std_thread")]
    if standard {
        let deadline = crate::group::shutdown_deadline(std::time::Instant::now(), every);
        crate::executor::sleep_until(deadline).await;
        return;
    }

    let _ = standard;

    #[cfg(feature = "tokio")]
    tokio::time::sleep(every).await;
}

impl From<DedicatedStartError<AktorFailure>> for AktorStartCause {
    fn from(error: DedicatedStartError<AktorFailure>) -> Self {
        match error {
            error
            @ (DedicatedStartError::Startup { .. } | DedicatedStartError::Observer { .. }) => {
                Self::Startup(Box::new(error.into()))
            }
            DedicatedStartError::Init(error) => Self::Init(error),
            DedicatedStartError::NotStarted => Self::Thread(DedicatedStartError::NotStarted),
            DedicatedStartError::Closed => Self::Thread(DedicatedStartError::Closed),
            DedicatedStartError::NoRuntime => Self::Thread(DedicatedStartError::NoRuntime),
            DedicatedStartError::InvalidCapacity => {
                Self::Thread(DedicatedStartError::InvalidCapacity)
            }
            DedicatedStartError::ThreadNotCreated(error) => {
                Self::Thread(DedicatedStartError::ThreadNotCreated(error))
            }
            DedicatedStartError::Thread(error) => Self::Thread(DedicatedStartError::Thread(error)),
            DedicatedStartError::Panicked { actor, cause } => {
                Self::Thread(DedicatedStartError::Panicked { actor, cause })
            }
        }
    }
}
