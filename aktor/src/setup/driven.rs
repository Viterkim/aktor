use super::*;
use crate::setup::intervals::{LocalInterval, LocalIntervals};
use crate::{
    ActorArgs, AktorError,
    local::{self, Request, clock::AktorGroupClock, hooks::AktorHooks},
    message::LocalFuture,
    operation::Operation,
};
use alloc::string::ToString;
use core::{future::poll_fn, ops::AsyncFnOnce, task::Poll};
#[cfg(feature = "embassy")]
use kind::EmbassyLocal;

#[cfg(feature = "embassy")]
impl<S: 'static, Start, Role> AktorStart for AktorNew<S, Start, EmbassyLocal, Role>
where
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    type Error = AktorStartError;
    type Group = crate::embassy::AktorGroup;
    type Handles = crate::embassy::Handle<S, 0, (), Role>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let spawn = self.kind.spawn.clone();
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            start_driven::<_, _, _, local::clock::Embassy>(setup, context, move |future| {
                spawn(future)
            })
            .await
            .map(local::Handle::with_role)
        })
    }
}
impl<S: 'static, Start, Clock: AktorGroupClock, Role> AktorStart
    for AktorNew<S, Start, kind::Local<Clock>, Role>
where
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    type Error = AktorStartError;
    type Group = local::AktorGroup<Clock>;
    type Handles = local::Handle<S, 0, (), Role, Clock>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn source_is_setup(error: &Self::Error) -> bool {
        error.source_is_setup()
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let spawn = self.kind.spawn.clone();
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            start_driven::<_, _, _, Clock>(setup, context, move |future| spawn(future))
                .await
                .map(local::Handle::with_role)
        })
    }
}

pub async fn start_driven<S: 'static, Start, Kind, Clock: AktorGroupClock>(
    setup: AktorNew<S, Start, Kind>,
    context: AktorStartContext<local::AktorGroup<Clock>>,
    spawn: impl Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>,
) -> Result<local::Handle<S, 0, (), (), Clock>, AktorStartError>
where
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation),
            End<S> = dyn AktorEnd<S>,
            Interval<S> = dyn AktorIntervalLogic<S>,
        >,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    start_custom(setup, context, spawn, local::serve_on::<Clock, _, 0, ()>).await
}

pub async fn start_custom<S: 'static, Start, Kind, Clock: AktorGroupClock, Runner>(
    setup: AktorNew<S, Start, Kind>,
    mut context: AktorStartContext<local::AktorGroup<Clock>>,
    spawn: impl Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>,
    runner: Runner,
) -> Result<local::Handle<S, 0, (), (), Clock>, AktorStartError>
where
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation),
            End<S> = dyn AktorEnd<S>,
            Interval<S> = dyn AktorIntervalLogic<S>,
        >,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
    Runner: for<'a> AsyncFnOnce(local::AktorRunner<'a, S>) -> Result<(), AktorError> + 'static,
{
    let options = setup.options;
    let AktorClosures {
        start,
        end,
        intervals,
        before_each,
        after_each,
    } = setup.closures;

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        return Err(AktorStartError::Setup(AktorError::new(
            "interval duration must be positive",
        )));
    }

    let LocalIntervals {
        scheduled: intervals,
        retained,
    } = LocalIntervals::new(intervals);
    let handle = context
        .group
        .spawn_with_custom(
            ActorArgs {
                name: setup.name.name,
                capacity: options.capacity,
                setup: async move || start().await,
                cleanup: async move |state| {
                    if let Some(mut end) = end {
                        end.0.run(state).await
                    } else {
                        drop(state);
                        Ok(())
                    }
                },
            },
            AktorHooks {
                before_each: before_each.map(|hook| hook.0),
                after_each: after_each.map(|hook| hook.0),
                intervals: retained,
            },
            Kind::EXECUTION,
            runner,
        )
        .map_err(|error| AktorStartError::Setup(AktorError::new(error.to_string())))?;

    handle.ready().await.map_err(AktorStartError::Local)?;

    for interval in intervals {
        (spawn)(schedule(
            interval,
            handle.new_handle(),
            context.group.killswitch(),
        ))
        .map_err(AktorStartError::Setup)?;
    }

    Ok(handle)
}

async fn while_running<S, Clock: AktorGroupClock, F: Future>(
    kill: &local::KillSwitch<Clock>,
    handle: &local::Handle<S, 0, (), (), Clock>,
    future: F,
) -> Option<F::Output> {
    let mut stopping = Box::pin(kill.wait_stopping());
    let mut closing = Box::pin(handle.wait_closing());
    let mut future = Box::pin(future);

    poll_fn(|cx| {
        if stopping.as_mut().poll(cx).is_ready() || closing.as_mut().poll(cx).is_ready() {
            Poll::Ready(None)
        } else {
            future.as_mut().poll(cx).map(Some)
        }
    })
    .await
}

fn schedule<S: 'static, Clock: AktorGroupClock>(
    interval: LocalInterval<S>,
    handle: local::Handle<S, 0, (), (), Clock>,
    kill: local::KillSwitch<Clock>,
) -> LocalFuture<'static, ()> {
    let callback = interval.callback;

    Box::pin(async move {
        loop {
            let deadline = Clock::deadline(interval.every);

            if while_running(&kill, &handle, Clock::wait(deadline))
                .await
                .is_none()
            {
                break;
            }

            let callback = callback.clone();
            let request = Request::new(
                &handle,
                Operation {
                    name: "interval",
                    caller: core::panic::Location::caller(),
                },
                async move |state: &mut S, ()| {
                    let Some(callback) = callback.upgrade() else {
                        return;
                    };
                    let work = callback.borrow_mut().take();

                    if let Some(mut work) = work {
                        work.0.run(state).await;
                        *callback.borrow_mut() = Some(work);
                    }
                },
                (),
            );

            if while_running(&kill, &handle, request.run_interval()).await != Some(true) {
                break;
            }
        }
    })
}
