use super::closures::AktorEndCall;
use super::*;
use crate::{
    Aktor,
    listener::hooks::CallHooks,
    message,
    operation::Operation,
    owner::{self, Launched},
    serve::{self, Started},
};
use parking_lot::Mutex;
use std::{ops::AsyncFnOnce, sync::Arc};

#[cfg(feature = "tokio")]
use super::kind::TokioThread;

#[cfg(feature = "tokio")]
impl<S, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, TokioThread, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<<TokioThread as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<TokioThread as crate::setup::AktorMode>::Each<S>>,
    S: 'static,
    End: AktorEndCall<S, EM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, X> + Send + 'static,
    X: Into<(E, String)> + 'static,
    E: Send + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E;
    type Handles = Aktor<S>;

    fn needs_tokio_group(&self) -> bool {
        true
    }

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group
            .start_threaded()
            .er_with(|error| error.start_reason())?;

        Ok(())
    }

    fn closed(_: &str, group: &crate::AktorGroup) -> Self::Handles {
        Aktor::closed_handle(group.killswitch())
    }

    fn launch(self, context: AktorStartContext<crate::AktorGroup>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        launch_thread(this, context, false)
    }
}
#[cfg(feature = "std_thread")]
impl<S, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, kind::StdThread, End, (EM, E), Before, After>
where
    Before: crate::setup::closures::EachHook<<kind::StdThread as crate::setup::AktorMode>::Each<S>>,
    After: crate::setup::closures::EachHook<<kind::StdThread as crate::setup::AktorMode>::Each<S>>,
    S: 'static,
    End: AktorEndCall<S, EM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, X> + Send + 'static,
    X: Into<(E, String)> + 'static,
    E: Send + 'static,
{
    type Group = crate::AktorGroup;
    type Error = E;
    type Handles = Aktor<S>;

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group
            .start_standard()
            .er_with(|error| error.start_reason())?;

        Ok(())
    }

    fn closed(_: &str, group: &crate::AktorGroup) -> Self::Handles {
        Aktor::closed_handle(group.killswitch())
    }

    fn launch(self, context: AktorStartContext<crate::AktorGroup>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        launch_thread(this, context, true)
    }
}

type ThreadReady<E> = AktorThreadFuture<Result<(), StartFailure<E>>>;
type ThreadLaunch<S, E> = Result<(Aktor<S>, ThreadReady<E>), StartFailure<E>>;

#[derive(Er)]
enum ThreadSetupErr {
    CapacityTooLarge,
    Cut,
    ObserverGone,
}
fn launch_thread<S, Start, Kind, End, CM, E>(
    setup: crate::setup::Prepared<S, Start, Kind, End, CM>,
    mut context: AktorStartContext<crate::AktorGroup>,
    standard: bool,
) -> ThreadLaunch<S, E>
where
    S: 'static,
    Kind: AktorMode<
            Leave = (),
            Each<S> = dyn FnMut(&mut S, Operation) + Send,
            Interval<S> = dyn AktorIntervalLogic<S> + Send,
        >,
    End: AktorEndCall<S, CM> + Send + 'static,
    Start: AsyncFnOnce() -> Result<S, (E, String)> + Send + 'static,
    E: Send + 'static,
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

    if capacity.get() > tokio::sync::Semaphore::MAX_PERMITS {
        let error = er_new!(
            LaunchError::new(AktorStartReason::InvalidSetup),
            ThreadSetupErr::CapacityTooLarge
        );
        return Err(validation_error(
            &context.group,
            &name,
            Kind::EXECUTION,
            error.er_split(),
        ));
    }

    let hooks = CallHooks {
        before_each,
        after_each,
    };
    let intervals = intervals
        .into_iter()
        .map(|interval| {
            let run = Arc::new(Mutex::new(Some(interval.run)));

            serve::Interval {
                every: interval.every,
                message: Box::new(move |due: serve::Due| {
                    let run = run.clone();

                    message::detached(INTERVAL, async move |state: &mut S, ()| {
                        let _due = due;
                        let work = run.lock().take();

                        if let Some(mut work) = work {
                            work.0.run(state).await;
                            *run.lock() = Some(work);
                        }
                    })
                })
                    as Box<dyn FnMut(serve::Due) -> message::Message<S> + Send>,
            }
        })
        .collect();

    let Launched {
        actor,
        started,
        joined,
    } = owner::launch(
        &mut context.group,
        name.clone(),
        capacity,
        standard,
        stuck,
        hooks,
        intervals,
        async move || start().await,
        move |state| end.run(state),
    )
    .er_with(|error| LaunchError::new(error.reason))?;

    context.group.keep(actor.clone());

    let group = context.group;
    let ready = Box::pin(async move {
        let completion = group.completion();
        let mut started = started;
        let result = tokio::select! {
            biased;
            result = &mut started => result.ok(),
            _ = completion.wait() => started.try_recv().ok(),
        };

        let failure = match result {
            Some(Ok(Started::Ready)) => return Ok(()),
            Some(Ok(Started::Init(failure))) => failure,
            Some(Ok(Started::Cut)) => er_new!(
                LaunchError::new(AktorStartReason::Stopping),
                ThreadSetupErr::Cut
            ),
            Some(Err(error)) => {
                er_new!(LaunchError::new(AktorStartReason::InvalidSetup), error)
            }
            None => {
                let kill = group.killswitch();
                let error = er_new!(ThreadSetupErr::ObserverGone);
                let reason = if kill.is_stopping() {
                    AktorStartReason::Stopping
                } else {
                    kill.fail_startup(crate::AktorFailureInput::actor(
                        name,
                        Kind::EXECUTION,
                        crate::AktorPhase::Start,
                        error.er_report(),
                    ));
                    AktorStartReason::InvalidSetup
                };

                er_new!(LaunchError::new(reason), error)
            }
        };

        let rollback = group.completion().wait().await;

        let mut joined = joined;
        match joined.try_recv() {
            Ok(Ok(())) | Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                Err(failure.into())
            }
            // A join can arrive after the first rollback report went out at grace.
            Ok(Err((id, message))) => {
                let failure = if rollback
                    .as_ref()
                    .is_ok_and(|report| report.failure(id).is_some())
                {
                    failure
                } else {
                    failure.er_save("join", message)
                };
                Err(failure.into())
            }
            Err(error) => Err(failure.er_add(error).into()),
        }
    });

    Ok((actor, ready))
}

const INTERVAL: Operation = Operation::INTERVAL;
