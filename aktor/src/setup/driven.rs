use super::*;
use crate::setup::intervals::{LocalInterval, LocalIntervals};
use crate::{
    AktorArgs,
    local::{self, Request, clock::AktorGroupClock, hooks::CallHooks},
    message::LocalFuture,
    operation::Operation,
};

use core::{future::poll_fn, ops::AsyncFnOnce, task::Poll};
#[cfg(feature = "embassy")]
use kind::EmbassyLocal;

#[cfg(feature = "embassy")]
impl<S: 'static, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, EmbassyLocal, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<<EmbassyLocal as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<EmbassyLocal as crate::setup::AktorMode>::Each<S>>,
    Start: AsyncFnOnce() -> Result<S, X> + 'static,
    X: Into<(E, String)> + 'static,
    E: 'static,
    End: crate::setup::closures::AktorEndCall<S, EM> + 'static,
{
    type Group = crate::embassy::AktorGroup;
    type Error = E;
    type Handles = crate::embassy::Handle<S, ()>;

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.spawn_driver(self.kind.spawn.clone())
    }

    fn closed(name: &str, group: &Self::Group) -> Self::Handles {
        closed_handle(name, group)
    }

    fn launch(self, context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        let spawn = this.kind.spawn.clone();

        launch_custom(
            this,
            context,
            move |future| spawn(future),
            local::serve_on::<local::clock::Embassy, _, ()>,
        )
    }
}
impl<S: 'static, Start, Clock: AktorGroupClock, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, kind::Local<Clock>, End, (EM, E), Before, After>
where
    Before:
        crate::setup::closures::EachHook<<kind::Local<Clock> as crate::setup::AktorMode>::Each<S>>,
    After:
        crate::setup::closures::EachHook<<kind::Local<Clock> as crate::setup::AktorMode>::Each<S>>,
    Start: AsyncFnOnce() -> Result<S, X> + 'static,
    X: Into<(E, String)> + 'static,
    E: 'static,
    End: crate::setup::closures::AktorEndCall<S, EM> + 'static,
{
    type Group = local::AktorGroup<Clock>;
    type Error = E;
    type Handles = local::Handle<S, (), Clock>;

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.spawn_driver(self.kind.spawn.clone())
    }

    fn closed(name: &str, group: &Self::Group) -> Self::Handles {
        closed_handle(name, group)
    }

    fn launch(self, context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        let spawn = this.kind.spawn.clone();

        launch_custom(
            this,
            context,
            move |future| spawn(future),
            local::serve_on::<Clock, _, ()>,
        )
    }
}

/// A handle whose actor never launched, calls on it find it closed.
pub fn closed_handle<S: 'static, Clock: AktorGroupClock>(
    name: &str,
    group: &local::AktorGroup<Clock>,
) -> local::Handle<S, (), Clock> {
    group.closed_handle(name)
}

type CustomLaunch<S, Clock, E> = Result<
    (
        local::Handle<S, (), Clock>,
        LocalFuture<'static, Result<(), StartFailure<E>>>,
    ),
    StartFailure<E>,
>;

pub fn launch_custom<S: 'static, Start, Kind, Clock: AktorGroupClock, Runner, End, CM, E>(
    setup: crate::setup::Prepared<S, Start, Kind, End, CM>,
    mut context: AktorStartContext<local::AktorGroup<Clock>>,
    spawn: impl Fn(LocalFuture<'static, ()>) -> Result<(), String>,
    runner: Runner,
) -> CustomLaunch<S, Clock, E>
where
    Kind:
        AktorMode<Each<S> = dyn FnMut(&mut S, Operation), Interval<S> = dyn AktorIntervalLogic<S>>,
    Start: AsyncFnOnce() -> Result<S, (E, String)> + 'static,
    E: 'static,
    End: crate::setup::closures::AktorEndCall<S, CM> + 'static,
    Runner: for<'a> AsyncFnOnce(local::AktorRunner<'a, S>) -> Result<(), (local::RunnerError, String)>
        + 'static,
{
    let capacity = setup.capacity;
    let start = setup.start;
    let end = setup.end;
    let AktorHooks {
        intervals,
        before_each,
        after_each,
    } = setup.hooks;

    let invalid = || LaunchError::new(AktorStartReason::InvalidSetup);

    let LocalIntervals {
        scheduled: intervals,
        retained,
    } = LocalIntervals::new(intervals);
    let (sent, initialized) = super::ready::channel();
    let kill = context.group.killswitch();
    let ready = super::ready::startup(
        move || kill.is_startup_aborted(),
        move |result| sent.send(result),
    );
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
                            let mut failure = LaunchError::init(error);
                            let primary = startup_kill.startup_error(
                                startup_name,
                                Kind::EXECUTION,
                                failure.1.clone(),
                            );

                            failure.0.failure_id = Some(primary);
                            Err((failure, primary))
                        }
                    };

                    (result, ready)
                },
                cleanup: async move |state| end.run(state).await,
            },
            CallHooks {
                before_each,
                after_each,
                intervals: retained,
            },
            Kind::EXECUTION,
            runner,
        )
        .er(invalid)?;

    for interval in intervals {
        if let Err(report) = spawn(schedule(interval, handle.clone())) {
            let failure = er_new!(invalid().er_save("spawner", || report));
            return Err(validation_error(
                &context.group,
                &name,
                Kind::EXECUTION,
                failure.into(),
            ));
        }
    }

    let completion = handle.completion();
    let kill = context.group.killswitch();
    let ready: LocalFuture<'static, Result<(), StartFailure<E>>> = Box::pin(async move {
        if let Some(result) = initialized.await {
            return result;
        }
        if let Some(failure) = kill.driver_loss() {
            return Err(failure);
        }

        match completion.observe().await {
            Err((error, report)) => er_bail!(
                LaunchError::new(
                    if matches!(*error.cause, crate::local::OwnerError::Cancelled) {
                        AktorStartReason::Stopping
                    } else {
                        AktorStartReason::Init
                    }
                ),
                (error.without_data(), report)
            ),
            Ok(()) => er_bail!(
                LaunchError::new(AktorStartReason::Stopping),
                LocalSetupErr::Stopped
            ),
        }
    });

    Ok((handle, ready))
}

async fn while_running<S, Clock: AktorGroupClock, F: Future>(
    handle: &local::Handle<S, (), Clock>,
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

fn schedule<S: 'static, Clock: AktorGroupClock>(
    interval: LocalInterval<S>,
    handle: local::Handle<S, (), Clock>,
) -> LocalFuture<'static, ()> {
    let callback = interval.callback;

    Box::pin(async move {
        if while_running(&handle, handle.observe_ready())
            .await
            .is_none_or(|result| result.is_err())
        {
            return;
        }

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
                Operation::INTERVAL,
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
