use super::*;
use crate::{
    ActorArgs, Aktor, AktorError,
    listener::{Handle, hooks::AktorHooks},
    message::call_async,
    operation::Operation,
};
use parking_lot::Mutex;
use std::{
    ops::AsyncFnOnce,
    sync::{Arc, Weak},
};

#[cfg(feature = "tokio")]
use super::kind::TokioThread;

#[cfg(feature = "tokio")]
impl<S, Start, Role, E> AktorStart for AktorNew<S, Start, TokioThread, Role>
where
    S: 'static,
    E: Send + 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
{
    type Error = AktorStartError<E>;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError<E>, AktorCleanupError, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, Self::Error>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

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
impl<S, Start, Role, E> AktorStart for AktorNew<S, Start, kind::StdThread, Role>
where
    S: 'static,
    E: Send + 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
{
    type Error = AktorStartError<E>;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError<E>, AktorCleanupError, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, Self::Error>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

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
#[cfg(feature = "tokio")]
impl<S, Start, Role, E, C> AktorStart
    for AktorNew<S, Start, kind::AktorLifecycle<TokioThread, C>, Role>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
{
    type Error = AktorStartError<E>;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError<E>, AktorCleanupError<C>, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, Self::Error>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

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
impl<S, Start, Role, E, C> AktorStart
    for AktorNew<S, Start, kind::AktorLifecycle<kind::StdThread, C>, Role>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
{
    type Error = AktorStartError<E>;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError<E>, AktorCleanupError<C>, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, Self::Error>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

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

async fn start_thread<S, Start, Kind, E, C>(
    setup: AktorNew<S, Start, Kind>,
    mut context: AktorStartContext<crate::AktorGroup>,
    standard: bool,
) -> Result<Aktor<S, AktorSetupError<E>, AktorCleanupError<C>>, AktorStartError<E>>
where
    S: 'static,
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation) + Send,
            End<S> = dyn AktorEnd<S, C> + Send,
            Interval<S> = dyn AktorIntervalLogic<S> + Send,
        >,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
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
        return Err(AktorStartError::Setup(AktorError::new(
            "interval duration must be positive",
        )));
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

    let end = Arc::new(Mutex::new(end));
    let actor = context
        .group
        .spawn_async_on(
            ActorArgs {
                name,
                capacity: options.capacity,
                setup: async move || start().await,
                cleanup: move |state| {
                    let end = end.clone();
                    let retained = retained.clone();

                    async move {
                        let _retained = retained;
                        let callback = end.lock().take();

                        if let Some(mut callback) = callback {
                            let result = callback.0.run(state).await;
                            *end.lock() = Some(callback);
                            result
                        } else {
                            drop(state);
                            Ok(())
                        }
                    }
                },
            },
            hooks,
            standard,
        )
        .await
        .map_err(AktorStartError::from)?;

    for interval in intervals {
        schedule(
            interval,
            actor.new_handle(),
            context.group.killswitch(),
            standard,
            context
                .group
                .spawner(standard)
                .map_err(|error| AktorStartError::Setup(AktorError::new(error.to_string())))?,
        )
        .map_err(|error| AktorStartError::Setup(AktorError::new(error.to_string())))?;
    }

    Ok(actor)
}

fn schedule<S: 'static>(
    interval: ThreadInterval<S>,
    handle: Handle<S>,
    kill: crate::KillSwitch,
    standard: bool,
    spawner: crate::executor::Spawner,
) -> std::io::Result<()> {
    let callback = interval.callback;

    spawner.spawn_named("aktor interval", async move {
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
    })
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

impl<Data: 'static> From<DedicatedStartError<AktorSetupError<Data>>> for AktorStartError<Data> {
    fn from(error: DedicatedStartError<AktorSetupError<Data>>) -> Self {
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
