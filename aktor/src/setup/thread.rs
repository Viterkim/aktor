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
impl<S, Start, Role> AktorStart for AktorSetup<S, Start, TokioThread, Role>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + Send + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError, AktorCleanupError, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
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
                .map_err(AktorStartError::from)
        })
    }
}

#[cfg(feature = "std_thread")]
impl<S, Start, Role> AktorStart for AktorSetup<S, Start, kind::StdThread, Role>
where
    S: 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + Send + 'static,
{
    type Error = AktorStartError;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError, AktorCleanupError, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, AktorStartError>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
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
                .map_err(AktorStartError::from)
        })
    }
}

#[cfg(feature = "tokio")]
impl<S, Start, Role, E, C> AktorStart
    for AktorSetup<S, Start, kind::AktorLifecycle<TokioThread, E, C>, Role>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
{
    type Error = AktorThreadStartError<E>;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError<E>, AktorCleanupError<C>, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, Self::Error>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
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
    for AktorSetup<S, Start, kind::AktorLifecycle<kind::StdThread, E, C>, Role>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
{
    type Error = AktorThreadStartError<E>;
    type Group = crate::AktorGroup;
    type Handles = Aktor<S, AktorSetupError<E>, AktorCleanupError<C>, Role>;
    type Startup = AktorThreadFuture<Result<Self::Handles, Self::Error>>;

    fn grace(&self) -> Duration {
        self.options
            .as_ref()
            .map_or(Duration::from_secs(5), |options| options.shutdown_grace)
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
    setup: AktorSetup<S, Start, Kind>,
    mut context: AktorStartContext<crate::AktorGroup>,
    standard: bool,
) -> Result<Aktor<S, AktorSetupError<E>, AktorCleanupError<C>>, AktorThreadStartError<E>>
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
    let AktorSetup {
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

    let options = options.unwrap_or_default();

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        return Err(AktorThreadStartError::Setup(AktorError::new(
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
        .map_err(AktorThreadStartError::Thread)?;

    for interval in intervals {
        schedule(
            interval,
            actor.new_handle(),
            context.group.killswitch(),
            standard,
            context.group.spawner(standard).map_err(|error| {
                AktorThreadStartError::Setup(AktorError::new(error.to_string()))
            })?,
        )
        .map_err(|error| AktorThreadStartError::Setup(AktorError::new(error.to_string())))?;
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

#[derive(Er)]
pub enum AktorThreadStartError<Data: 'static> {
    #[er(format = "{0}")]
    Setup(#[er(source)] AktorSetupError),
    #[er(format = "{0}")]
    Thread(#[er(source)] DedicatedStartError<AktorSetupError<Data>>),
}
impl<Data: 'static> From<AktorSetupError> for AktorThreadStartError<Data> {
    fn from(error: AktorSetupError) -> Self {
        Self::Setup(error)
    }
}
impl From<AktorThreadStartError<()>> for AktorStartError {
    fn from(error: AktorThreadStartError<()>) -> Self {
        match error {
            AktorThreadStartError::Setup(error) => Self::Setup(error),
            AktorThreadStartError::Thread(error) => Self::Thread(error),
        }
    }
}
