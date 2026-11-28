use super::super::*;
use super::Completion;
use crate::panic::panic_tree;
use crate::setup::intervals::{LocalInterval, LocalIntervals};
use crate::{
    AktorFailureInput, AktorStuck, OwnerOutcome,
    group::shutdown::contain_drop,
    local::{self, Handle, hooks::CallHooks},
    message::LocalFuture,
    operation::Operation,
    setup::closures::AktorEndCall,
};
use core::ops::AsyncFnOnce;
use er::*;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use tokio::sync::{oneshot, watch};

#[derive(Er)]
enum LocalOwnerErr {
    ObserverGone,
}
impl Completion {
    fn publish(&mut self, outcome: OwnerOutcome) {
        if let Some(completed) = self.completed.take() {
            let _sent = completed.send(outcome);
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if self.completed.is_some() {
            // This sits in the same future as the state, the executor dropping one drops both.
            self.publish(OwnerOutcome {
                retirement: crate::AktorRetirement::Terminated,
                init: crate::AktorInitEvidence::Unknown,
                ran_past_grace: false,
            });
        }
    }
}

pub fn closed_local_handle<S: 'static, Clock: 'static>(
    name: &str,
    group: &crate::AktorGroup,
    standard: bool,
) -> Handle<S, (), Clock> {
    let (handle, owner) = local::closed_channel::<S, Clock>();
    handle.manage_on(
        name.into(),
        crate::AktorExecution::Local,
        group.killswitch(),
        standard,
    );
    drop(owner);
    handle
}

type LocalReady<E> = oneshot::Receiver<Result<(), StartFailure<E>>>;
type LocalLaunch<S, Clock, E> = Result<(Handle<S, (), Clock>, LocalReady<E>), StartFailure<E>>;

pub fn launch_local<S, Start, Kind, Clock, End, CM, E>(
    setup: crate::setup::Prepared<S, Start, Kind, End, CM>,
    mut context: AktorStartContext<crate::AktorGroup>,
    spawn: impl Fn(LocalFuture<'static, ()>),
    sleep: fn(Duration) -> LocalFuture<'static, ()>,
) -> LocalLaunch<S, Clock, E>
where
    S: 'static,
    Clock: 'static,
    Kind: AktorMode<
            Leave = core::convert::Infallible,
            Each<S> = dyn FnMut(&mut S, Operation),
            Interval<S> = dyn AktorIntervalLogic<S>,
        >,
    Start: AsyncFnOnce() -> Result<S, (E, String)> + 'static,
    E: 'static,
    End: AktorEndCall<S, CM> + 'static,
{
    let AktorNew {
        name,
        kind: _,
        start,
        end,
        hooks,
        capacity,
        stuck: AktorStuck::Kill,
    } = setup;
    let AktorHooks {
        intervals,
        before_each,
        after_each,
    } = hooks;

    let LocalIntervals {
        scheduled: intervals,
        retained,
    } = LocalIntervals::new(intervals);
    let (handle, mut owner) = local::channel_with_clock::<S, (), Clock>(capacity);

    owner.hooks = CallHooks {
        before_each,
        after_each,
        intervals: retained,
    };

    let kill = context.group.killswitch();

    handle.manage_on(
        name.clone(),
        Kind::EXECUTION,
        kill.clone(),
        Kind::EXECUTION == crate::AktorExecution::BevyLocal,
    );

    let control = handle.downgrade();
    let (closing, mut closed) = watch::channel(false);
    let (forcing, mut forced) = watch::channel(false);
    let (completed, outcome) = oneshot::channel();
    let label = name.clone();
    let observer = kill.clone();

    context
        .group
        .register_owner(
            name.clone(),
            Kind::EXECUTION,
            Box::new(move || {
                closing.send_replace(true);
            }),
            Box::new(move || {
                forcing.send_replace(true);
            }),
            None,
            None,
            Box::pin(async move {
                outcome.await.unwrap_or_else(|_| {
                    observer.fail(AktorFailureInput::actor(
                        label.clone(),
                        Kind::EXECUTION,
                        crate::AktorPhase::Join,
                        er_new!(LocalOwnerErr::ObserverGone).er_report(),
                    ));
                    OwnerOutcome {
                        retirement: crate::AktorRetirement::StillRunning,
                        init: crate::AktorInitEvidence::Unknown,
                        ran_past_grace: false,
                    }
                })
            }),
        )
        .er_with(|error| LaunchError::new(error.start_reason()))?;

    let mut completion = Completion {
        name,
        kill: kill.clone(),
        completed: Some(completed),
    };

    let (sent, initialized) = oneshot::channel();
    let receipt = alloc::rc::Rc::new(core::cell::RefCell::new(Some(sent)));
    let publishing = receipt.clone();
    let checking = kill.clone();
    let ready = super::super::ready::startup(
        move || checking.is_startup_aborted(),
        move |result| {
            let sender = publishing.borrow_mut().take();
            if let Some(sent) = sender {
                let _sent = sent.send(result);
            }
        },
    );
    let startup_kill = kill.clone();
    let startup_name = completion.name.clone();

    spawn(Box::pin(async move {
        let retained = owner.completion();
        let mut driver = Box::pin(
            AssertUnwindSafe(owner.run_prepared(
                async move || {
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
                async move |state| end.run(state).await,
                async |runner| {
                    #[cfg(target_family = "wasm")]
                    {
                        local::serve_browser(runner).await
                    }
                    #[cfg(not(target_family = "wasm"))]
                    {
                        local::serve(runner).await
                    }
                },
            ))
            .catch_unwind(),
        );

        let mut stopping = false;
        let mut cancelling = false;
        let mut report = OwnerOutcome {
            retirement: crate::AktorRetirement::StillRunning,
            init: crate::AktorInitEvidence::Unknown,
            ran_past_grace: false,
        };

        loop {
            tokio::select! {
                biased;
                result = &mut driver => {
                    if let Err(payload) = result {
                        if !payload.is::<local::panic::Reported>() {
                            completion.kill.fail(AktorFailureInput::actor(
                                completion.name.clone(),
                                Kind::EXECUTION,
                                crate::AktorPhase::Runner,
                                panic_tree(&payload).er_report(),
                            ));
                        }
                        contain_drop(payload, &completion.kill, crate::AktorFailureSource::Actor { id: (),
                            name: completion.name.clone(),
                            kind: Kind::EXECUTION,
                            phase: crate::AktorPhase::Drop,
                        });
                    }
                    break;
                },
                changed = closed.changed(), if !stopping => {
                    if changed.is_err() || *closed.borrow_and_update() {
                        stopping = true;
                        control.shutdown();
                    }
                },
                // Its grace is up, the calls go and `end` still gets its own grace.
                changed = forced.changed(), if !cancelling => {
                    if changed.is_err() || *forced.borrow_and_update() {
                        cancelling = true;
                        control.cancel_calls();
                    }
                },
            }
        }

        report.ran_past_grace = retained.ran_past_grace();
        report.retirement = if retained.cleanup_completed() {
            crate::AktorRetirement::Released
        } else {
            crate::AktorRetirement::Terminated
        };
        report.init = retained.init();
        contain_drop(
            driver,
            &completion.kill,
            crate::AktorFailureSource::Actor {
                id: (),
                name: completion.name.clone(),
                kind: Kind::EXECUTION,
                phase: crate::AktorPhase::Drop,
            },
        );
        drop(control);

        let sender = receipt.borrow_mut().take();
        if let Some(sent) = sender {
            let failure = match retained.observe().await {
                Err((error, report)) => er_new!(
                    LaunchError::new(
                        if matches!(*error.cause, crate::local::OwnerError::Cancelled) {
                            AktorStartReason::Stopping
                        } else {
                            AktorStartReason::Init
                        }
                    ),
                    (error.without_data(), report)
                ),
                Ok(()) => er_new!(
                    LaunchError::new(AktorStartReason::Stopping),
                    LocalSetupErr::Stopped
                ),
            };
            let _sent = sent.send(Err(failure.into()));
        }

        completion.publish(report);
    }));

    for interval in intervals {
        spawn(schedule(interval, handle.downgrade(), sleep));
    }

    spawn(crate::group::keeper(
        handle.clone(),
        context.group.completion(),
    ));

    Ok((handle, initialized))
}

pub async fn readiness<E: 'static>(
    mut initialized: LocalReady<E>,
    completion: crate::AktorGroupCompletion,
) -> Result<(), StartFailure<E>> {
    let result = tokio::select! {
        biased;
        result = &mut initialized => result.ok(),
        _ = completion.wait() => initialized.try_recv().ok(),
    };

    match result {
        Some(result) => result,
        None => er_bail!(
            LaunchError::new(AktorStartReason::Stopping),
            LocalSetupErr::Stopped
        ),
    }
}

fn schedule<S: 'static, Clock: 'static>(
    interval: LocalInterval<S>,
    handle: local::WeakHandle<S, (), Clock>,
    sleep: fn(Duration) -> LocalFuture<'static, ()>,
) -> LocalFuture<'static, ()> {
    let every = interval.every;
    let callback = interval.callback;

    Box::pin(async move {
        let Some(starting) = handle.upgrade() else {
            return;
        };
        tokio::select! {
            biased;
            _ = starting.wait_closing() => return,
            ready = starting.observe_ready() => if ready.is_err() { return },
        }
        drop(starting);

        loop {
            tokio::select! {
                biased;
                _ = handle.wait_closing() => break,
                _ = sleep(every) => {},
            }

            let Some(strong) = handle.upgrade() else {
                break;
            };
            let callback = callback.clone();
            let operation = Operation::INTERVAL;
            let request = local::Request::new(
                &strong,
                operation,
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

            tokio::select! {
                biased;
                _ = strong.wait_closing() => break,
                admitted = request.run_interval() => {
                    if !admitted { break; }
                },
            }
        }
    })
}
