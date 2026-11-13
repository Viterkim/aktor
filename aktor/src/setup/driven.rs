use super::*;
use crate::setup::intervals::{LocalInterval, LocalIntervals};
use crate::{
    AktorArgs, AktorError,
    local::{self, Request, clock::AktorGroupClock, hooks::CallHooks},
    message::LocalFuture,
    operation::Operation,
};
use alloc::string::ToString;
use core::{future::poll_fn, ops::AsyncFnOnce, task::Poll};
#[cfg(feature = "embassy")]
use kind::EmbassyLocal;

#[cfg(feature = "embassy")]
impl<S: 'static, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, EmbassyLocal, End, (CM, E, M)>
where
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = crate::embassy::AktorGroup;
    type Error = E::Error;
    type Handles = crate::embassy::Handle<S, 0, ()>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
        .map_err(|report| AktorStartCause::Setup(AktorError::new(report, ())))
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let spawn = self.kind.spawn.clone();
        let setup = self;

        Box::pin(async move {
            start_driven::<_, _, _, local::clock::Embassy, _, _, _, _>(
                setup,
                context,
                move |future| spawn(future).map_err(|report| AktorError::new(report, ())),
            )
            .await
        })
    }
}
impl<S: 'static, Start, Clock: AktorGroupClock, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, kind::Local<Clock>, End, (CM, E, M)>
where
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    type Group = local::AktorGroup<Clock>;
    type Error = E::Error;
    type Handles = local::Handle<S, 0, (), (), Clock>;
    type Startup = LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        let driver = group.listen()?;

        (self.kind.spawn)(Box::pin(async move {
            driver.await;
        }))
        .map_err(|report| AktorStartCause::Setup(AktorError::new(report, ())))
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        let spawn = self.kind.spawn.clone();
        let setup = self;

        Box::pin(async move {
            start_driven::<_, _, _, Clock, _, _, _, _>(setup, context, move |future| {
                spawn(future).map_err(|report| AktorError::new(report, ()))
            })
            .await
        })
    }
}

pub async fn start_driven<S: 'static, Start, Kind, Clock: AktorGroupClock, End, CM, E, M>(
    setup: AktorNew<S, Start, Kind, End, (CM, E, M)>,
    context: AktorStartContext<local::AktorGroup<Clock>>,
    spawn: impl Fn(LocalFuture<'static, ()>) -> Result<(), AktorError>,
) -> Result<local::Handle<S, 0, (), (), Clock>, AktorStartError<E::Error>>
where
    Kind:
        AktorMode<Each<S> = dyn FnMut(&mut S, Operation), Interval<S> = dyn AktorIntervalLogic<S>>,
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
{
    start_custom(setup, context, spawn, local::serve_on::<Clock, _, 0, ()>).await
}

pub async fn start_custom<S: 'static, Start, Kind, Clock: AktorGroupClock, Runner, End, CM, E, M>(
    setup: AktorNew<S, Start, Kind, End, (CM, E, M)>,
    mut context: AktorStartContext<local::AktorGroup<Clock>>,
    spawn: impl Fn(LocalFuture<'static, ()>) -> Result<(), AktorError>,
    runner: Runner,
) -> Result<local::Handle<S, 0, (), (), Clock>, AktorStartError<E::Error>>
where
    Kind:
        AktorMode<Each<S> = dyn FnMut(&mut S, Operation), Interval<S> = dyn AktorIntervalLogic<S>>,
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
    Runner:
        for<'a> AsyncFnOnce(local::AktorRunner<'a, S, 0, ()>) -> Result<(), AktorError> + 'static,
{
    let capacity = setup.capacity;
    let start = setup.start;
    let end = setup.end;
    let AktorHooks {
        intervals,
        before_each,
        after_each,
    } = setup.hooks;

    if intervals.iter().any(|interval| interval.every.is_zero()) {
        return Err(AktorStartError::not_run(validation_error(
            &context.group,
            &setup.name,
            Kind::EXECUTION,
            AktorStartCause::Setup(AktorError::new("interval duration must be positive", ())),
        )));
    }

    if capacity == 0 {
        return Err(AktorStartError::not_run(validation_error(
            &context.group,
            &setup.name,
            Kind::EXECUTION,
            AktorStartCause::Setup(AktorError::new(
                crate::message::AktorStateError::InvalidCapacity.to_string(),
                (),
            )),
        )));
    }

    let LocalIntervals {
        scheduled: intervals,
        retained,
    } = LocalIntervals::new(intervals);
    let (ready, initialized) = super::ready::channel();
    let startup_kill = context.group.killswitch();
    let name = setup.name.clone();
    let startup_name = name.clone();
    let handle = context
        .group
        .spawn_with_custom(
            AktorArgs {
                name: setup.name,
                capacity,
                setup: async move || {
                    let result = match start().await {
                        Ok(state) => Ok(state),
                        Err(error) => {
                            let (data, report) = error.split();
                            let failure = startup_kill.startup_error(
                                startup_name,
                                Kind::EXECUTION,
                                crate::AktorError::new(report, ()),
                            );
                            Err((data, failure))
                        }
                    };

                    (result, ready)
                },
                cleanup: async move |state| end.run(state).await,
            },
            CallHooks {
                before_each: before_each.map(|hook| hook.0),
                after_each: after_each.map(|hook| hook.0),
                intervals: retained,
            },
            Kind::EXECUTION,
            runner,
        )
        .map_err(|error| {
            AktorStartError::not_run(AktorStartCause::Setup(AktorError::new(
                error.to_string(),
                (),
            )))
        })?;

    match initialized.await {
        Some(result) => result?,
        None => {
            let (error, report) = handle.completion().wait().await.err().unwrap_or_else(|| {
                local::OwnerError::Runner(AktorError::new("local actor stopped during startup", ()))
                    .into()
            });

            return Err(AktorStartCause::Local(error, report).into());
        }
    }

    for interval in intervals {
        (spawn)(schedule(interval, handle.clone())).map_err(|error| {
            validation_error(
                &context.group,
                &name,
                Kind::EXECUTION,
                AktorStartCause::Setup(error),
            )
        })?;
    }

    Ok(handle)
}

async fn while_running<S, Clock: AktorGroupClock, F: Future, C>(
    handle: &local::Handle<S, 0, C, (), Clock>,
    future: F,
) -> Option<F::Output> {
    let mut closing = Box::pin(handle.wait_closing());
    let mut future = Box::pin(future);

    poll_fn(|cx| {
        if closing.as_mut().poll(cx).is_ready() {
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
) -> LocalFuture<'static, ()> {
    let callback = interval.callback;

    Box::pin(async move {
        loop {
            let deadline = Clock::deadline(interval.every);

            if while_running(&handle, Clock::wait(deadline))
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

            if while_running(&handle, request.run_interval()).await != Some(true) {
                break;
            }
        }
    })
}
