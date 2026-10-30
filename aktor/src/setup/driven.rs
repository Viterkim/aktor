use super::*;
use crate::setup::intervals::{LocalInterval, LocalIntervals};
use crate::{
    AktorArgs, AktorError,
    local::{self, Request, clock::AktorGroupClock, hooks::AktorHooks},
    message::LocalFuture,
    operation::Operation,
};
use alloc::string::ToString;
use core::{future::poll_fn, ops::AsyncFnOnce, task::Poll};
#[cfg(feature = "embassy")]
use kind::EmbassyLocal;

#[cfg(feature = "embassy")]
impl<S: 'static, Start, Role, End, CM> AktorStart<CM>
    for AktorNew<S, Start, EmbassyLocal, Role, End>
where
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::embassy::AktorGroup;
    type Handles = crate::embassy::Handle<S, 0, (), Role>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

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
            start_driven::<_, _, _, local::clock::Embassy, _, _>(setup, context, move |future| {
                spawn(future)
            })
            .await
            .map(local::Handle::with_role)
        })
    }
}
impl<S: 'static, Start, Clock: AktorGroupClock, Role, End, CM> AktorStart<CM>
    for AktorNew<S, Start, kind::Local<Clock>, Role, End>
where
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = local::AktorGroup<Clock>;
    type Handles = local::Handle<S, 0, (), Role, Clock>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

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
            start_driven::<_, _, _, Clock, _, _>(setup, context, move |future| spawn(future))
                .await
                .map(local::Handle::with_role)
        })
    }
}

pub async fn start_driven<S: 'static, Start, Kind, Clock: AktorGroupClock, End, CM>(
    setup: AktorNew<S, Start, Kind, AktorNoRole, End>,
    context: AktorStartContext<local::AktorGroup<Clock>>,
    spawn: impl Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>,
) -> Result<local::Handle<S, 0, (), (), Clock>, AktorStartError>
where
    Kind:
        AktorMode<Each<S> = dyn FnMut(&mut S, Operation), Interval<S> = dyn AktorIntervalLogic<S>>,
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    start_custom(setup, context, spawn, local::serve_on::<Clock, _, 0, ()>).await
}

pub async fn start_custom<S: 'static, Start, Kind, Clock: AktorGroupClock, Runner, End, CM>(
    setup: AktorNew<S, Start, Kind, AktorNoRole, End>,
    mut context: AktorStartContext<local::AktorGroup<Clock>>,
    spawn: impl Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>,
    runner: Runner,
) -> Result<local::Handle<S, 0, (), (), Clock>, AktorStartError>
where
    Kind:
        AktorMode<Each<S> = dyn FnMut(&mut S, Operation), Interval<S> = dyn AktorIntervalLogic<S>>,
    Start: AsyncFnOnce() -> Result<S, crate::AktorSetupError> + 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
    Runner:
        for<'a> AsyncFnOnce(local::AktorRunner<'a, S, 0, ()>) -> Result<(), AktorError> + 'static,
{
    let options = setup.options;
    let AktorClosures {
        start,
        mut end,
        intervals,
        before_each,
        after_each,
    } = setup.closures;

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        return Err(validation_error(
            &context.group,
            &setup.name.name,
            Kind::EXECUTION,
            AktorStartError::Setup(AktorError::new("interval duration must be positive", ())),
        ));
    }

    if options.capacity == 0 {
        return Err(validation_error(
            &context.group,
            &setup.name.name,
            Kind::EXECUTION,
            AktorStartError::Setup(AktorError::new(
                crate::message::AktorStateError::InvalidCapacity.to_string(),
                (),
            )),
        ));
    }

    let LocalIntervals {
        scheduled: intervals,
        retained,
    } = LocalIntervals::new(intervals);
    let (ready, initialized) = super::ready::channel();
    let startup_kill = context.group.killswitch();
    let name = setup.name.name.clone();
    let startup_name = name.clone();
    let handle = context
        .group
        .spawn_with_custom(
            AktorArgs {
                name: setup.name.name,
                capacity: options.capacity,
                setup: async move || {
                    let result = match start().await {
                        Ok(state) => Ok(state),
                        Err(error) => {
                            Err(startup_kill.startup_error(startup_name, Kind::EXECUTION, error))
                        }
                    };

                    (result, ready)
                },
                cleanup: async move |state| end.run(state).await,
            },
            AktorHooks {
                before_each: before_each.map(|hook| hook.0),
                after_each: after_each.map(|hook| hook.0),
                intervals: retained,
            },
            Kind::EXECUTION,
            runner,
        )
        .map_err(|error| AktorStartError::Setup(AktorError::new(error.to_string(), ())))?;

    match initialized.await {
        Some(result) => result?,
        None => {
            let error = handle.completion().wait().await.err().unwrap_or_else(|| {
                local::OwnerFailure::Runner(AktorError::new(
                    "local actor stopped during startup",
                    (),
                ))
                .into()
            });

            return Err(AktorStartError::Local(error));
        }
    }

    for interval in intervals {
        (spawn)(schedule(
            interval,
            handle.new_handle(),
            context.group.killswitch(),
        ))
        .map_err(|error| {
            validation_error(
                &context.group,
                &name,
                Kind::EXECUTION,
                AktorStartError::Setup(error),
            )
        })?;
    }

    Ok(handle)
}

async fn while_running<S, Clock: AktorGroupClock, F: Future, C>(
    kill: &local::KillSwitch<Clock>,
    handle: &local::Handle<S, 0, C, (), Clock>,
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

fn schedule<S: 'static, Clock: AktorGroupClock, C: 'static>(
    interval: LocalInterval<S>,
    handle: local::Handle<S, 0, C, (), Clock>,
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
