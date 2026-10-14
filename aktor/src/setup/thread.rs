use super::closures::AktorEndCall;
use super::*;
use crate::{
    ActorArgs, Aktor, AktorError, AktorFailure,
    listener::{Handle, hooks::AktorHooks},
    message::call_async,
    operation::Operation,
};
use parking_lot::Mutex;
use std::{
    ops::AsyncFnOnce,
    sync::{Arc, Weak},
};

#[cfg(all(test, feature = "std_thread"))]
mod tests;

#[cfg(feature = "tokio")]
use super::kind::TokioThread;

#[cfg(feature = "tokio")]
impl<S, Start, Role, End, CM> AktorStart<CM> for AktorNew<S, Start, TokioThread, Role, End>
where
    S: 'static,
    End: AktorEndCall<S, CM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + Send + 'static,
{
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start_threaded().map(|_| ())
    }

    fn start_in(self, context: AktorStartContext<crate::AktorGroup>) -> Self::Startup {
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            start_thread(setup, context, false)
                .await
                .map(Aktor::with_role)
        })
    }
}
#[cfg(feature = "std_thread")]
impl<S, Start, Role, End, CM> AktorStart<CM> for AktorNew<S, Start, kind::StdThread, Role, End>
where
    S: 'static,
    End: AktorEndCall<S, CM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + Send + 'static,
{
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start_standard().map(|_| ())
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            start_thread(setup, context, true)
                .await
                .map(Aktor::with_role)
        })
    }
}
struct ThreadInterval<S> {
    every: Duration,
    callback: Weak<Mutex<Option<AktorClosure<dyn AktorIntervalLogic<S> + Send>>>>,
}

async fn start_thread<S, Start, Kind, End, CM>(
    setup: AktorNew<S, Start, Kind, AktorNoRole, End>,
    mut context: AktorStartContext<crate::AktorGroup>,
    standard: bool,
) -> Result<Aktor<S>, AktorStartError>
where
    S: 'static,
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation) + Send,
            Interval<S> = dyn AktorIntervalLogic<S> + Send,
        >,
    End: AktorEndCall<S, CM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + Send + 'static,
{
    let AktorNew {
        name,
        closures,
        options,
        ..
    } = setup;

    let name = name.name;
    let AktorClosures {
        start,
        end,
        intervals,
        before_each,
        after_each,
    } = closures;

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        return Err(validation_error(
            &context.group,
            &name,
            Kind::EXECUTION,
            AktorStartError::Setup(AktorError::new("interval duration must be positive", ())),
        ));
    }

    if options.capacity == 0 || options.capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(validation_error(
            &context.group,
            &name,
            Kind::EXECUTION,
            AktorStartError::Thread(crate::listener::DedicatedStartError::InvalidCapacity),
        ));
    }

    let hooks = AktorHooks {
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

    let end = Arc::new(tokio::sync::Mutex::new(end));
    let startup_kill = context.group.killswitch();
    let startup_name = name.clone();
    let actor = context
        .group
        .spawn_async_initial_on_reported::<S, _, _, _, _, _>(
            ActorArgs {
                name: name.clone(),
                capacity: options.capacity,
                setup: async move || match start().await {
                    Ok(state) => Ok(state),
                    Err(error) => {
                        Err(startup_kill.startup_error(startup_name, Kind::EXECUTION, error))
                    }
                },
                cleanup: move |state| {
                    let end = end.clone();
                    let retained = retained.clone();

                    async move {
                        let _retained = retained;
                        let mut callback = end.lock().await;
                        callback.run(state).await
                    }
                },
            },
            hooks,
            standard,
            |error| error.diagnostics.clone(),
            |error| (error.diagnostics.clone(), error.primary),
        )
        .await
        .map_err(AktorStartError::from)?;

    for interval in intervals {
        schedule(
            interval,
            actor.new_handle(),
            context.group.killswitch(),
            standard,
            &context.group,
            context.group.spawner(standard).map_err(|error| {
                validation_error(
                    &context.group,
                    &name,
                    Kind::EXECUTION,
                    AktorStartError::Setup(AktorError::new(error.to_string(), ())),
                )
            })?,
        )
        .map_err(|error| {
            validation_error(
                &context.group,
                &name,
                Kind::EXECUTION,
                AktorStartError::Setup(AktorError::new(error.to_string(), ())),
            )
        })?;
    }

    Ok(actor)
}

fn schedule<S: 'static>(
    interval: ThreadInterval<S>,
    handle: Handle<S>,
    kill: crate::KillSwitch,
    standard: bool,
    group: &crate::AktorGroup,
    spawner: crate::executor::Spawner,
) -> std::io::Result<()> {
    let callback = interval.callback;
    let lifetime = kill.track_thread("aktor interval".into());

    let work = async move {
        let _lifetime = lifetime;

        loop {
            tokio::select! {
                biased;
                _ = kill.wait_stopping() => break,
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
                _ = kill.wait_stopping() => break,
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

impl From<DedicatedStartError<AktorFailure>> for AktorStartError {
    fn from(error: DedicatedStartError<AktorFailure>) -> Self {
        match error {
            DedicatedStartError::Init(error) => Self::Init(error),
            DedicatedStartError::NotStarted => Self::Thread(DedicatedStartError::NotStarted),
            DedicatedStartError::Closed => Self::Thread(DedicatedStartError::Closed),
            DedicatedStartError::NoRuntime => Self::Thread(DedicatedStartError::NoRuntime),
            DedicatedStartError::InvalidCapacity => {
                Self::Thread(DedicatedStartError::InvalidCapacity)
            }
            DedicatedStartError::Thread(error) => Self::Thread(DedicatedStartError::Thread(error)),
            DedicatedStartError::Panicked { actor, cause } => {
                Self::Thread(DedicatedStartError::Panicked { actor, cause })
            }
        }
    }
}
