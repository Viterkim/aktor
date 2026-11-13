use super::super::*;
use super::Completion;
use crate::setup::intervals::{LocalInterval, LocalIntervals};
use crate::{
    AktorError, AktorFailureReport, AktorOutcome,
    group::shutdown::{contain_drop, panic_message},
    local::{self, Handle, hooks::CallHooks},
    message::LocalFuture,
    operation::Operation,
    setup::closures::AktorEndCall,
};
use alloc::format;
use core::ops::AsyncFnOnce;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use tokio::sync::{oneshot, watch};

impl Completion {
    fn publish(&mut self, outcome: AktorOutcome) {
        if let Some(completed) = self.completed.take() {
            let _sent = completed.send(outcome);
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if self.completed.is_some() {
            let primary = self.kill.fail(AktorFailureReport {
                kind: Some(self.kind),
                actor: self.name.clone(),
                phase: "owner".into(),
                message: "local actor task cancelled".into(),
            });

            self.publish(AktorOutcome {
                retirement: crate::AktorRetireStatus::Unconfirmed,
                kind: self.kind,
                actor: self.name.clone(),
                diagnostics: if primary {
                    Vec::new()
                } else {
                    vec![AktorError::new("local actor task cancelled", ())]
                },
                ran_past_grace: false,
            });
        }
    }
}

pub async fn start_local<S, Start, Kind, Clock, End, CM, E, M>(
    setup: AktorNew<S, Start, Kind, End, (CM, E, M)>,
    mut context: AktorStartContext<crate::AktorGroup>,
    spawn: impl Fn(LocalFuture<'static, ()>),
    sleep: fn(Duration) -> LocalFuture<'static, ()>,
) -> Result<Handle<S, 0, (), (), Clock>, AktorStartError<E::Error>>
where
    S: 'static,
    Clock: 'static,
    Kind:
        AktorMode<Each<S> = dyn FnMut(&mut S, Operation), Interval<S> = dyn AktorIntervalLogic<S>>,
    Start: AsyncFnOnce() -> Result<S, E> + 'static,
    E: InitError<M> + 'static,
    E::Error: 'static,
    End: AktorEndCall<S, CM> + 'static,
{
    let AktorNew {
        name,
        kind: _,
        start,
        end,
        hooks,
        capacity,
        // It runs on your own thread, there's nothing to kill it separately from.
        stuck: _,
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

    if capacity == 0 {
        return Err(AktorStartError::not_run(validation_error(
            &context.group,
            &name,
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
    let (handle, mut owner) =
        local::channel_with_clock::<S, 0, (), Clock>(capacity).map_err(|error| {
            AktorStartError::not_run(AktorStartCause::Setup(AktorError::new(
                error.to_string(),
                (),
            )))
        })?;

    owner.hooks = CallHooks {
        before_each: before_each.map(|hook| hook.0),
        after_each: after_each.map(|hook| hook.0),
        intervals: retained,
    };

    let kill = context.group.killswitch();

    handle.manage_on(
        name.clone(),
        kill.clone(),
        Kind::EXECUTION == crate::AktorExecution::BevyLocal,
    );

    let alive = handle.clone();
    let (closing, mut closed) = watch::channel(false);
    let (forcing, mut forced) = watch::channel(false);
    let (completed, outcome) = oneshot::channel();
    let label = name.clone();

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
                outcome.await.unwrap_or_else(|_| AktorOutcome {
                    retirement: crate::AktorRetireStatus::Unconfirmed,
                    kind: Kind::EXECUTION,
                    actor: label,
                    diagnostics: vec![AktorError::new("local actor observer stopped", ())],
                    ran_past_grace: false,
                })
            }),
        )
        .map_err(|error| AktorStartError::not_run(AktorStartCause::Group(error)))?;

    let mut completion = Completion {
        name,
        kind: Kind::EXECUTION,
        kill: kill.clone(),
        completed: Some(completed),
    };

    let (ready, initialized) = super::super::ready::channel();
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
        let mut report = AktorOutcome {
            retirement: crate::AktorRetireStatus::Unconfirmed,
            kind: Kind::EXECUTION,
            actor: completion.name.clone(),
            diagnostics: Vec::new(),
            ran_past_grace: false,
        };

        loop {
            tokio::select! {
                biased;
                result = &mut driver => {
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err((error, _))) => {
                            match error {
                                local::OwnerError::SetupDiagnostic(_, true) => {},
                                local::OwnerError::SetupDiagnostic(error, false) => report.diagnostics.push(error),
                                local::OwnerError::CleanupFailure(error) => {
                                    if !error.primary {
                                        report.diagnostics.push(AktorError::new(error.diagnostics, ()));
                                    }
                                },
                                local::OwnerError::CleanupUnconfirmed => {},
                                _ => report.diagnostics.push(AktorError::new(format!("{error:#}"), ())),
                            }
                        },
                        Err(payload) => {
                            let message = panic_message(&payload);

                            let first = completion.kill.fail(AktorFailureReport {
                                kind: Some(Kind::EXECUTION),
                                actor: report.actor.clone(),
                                phase: "operation".into(),
                                message: message.clone(),
                            });
                            if !first && !retained.panic_reported() {
                                report.diagnostics.push(AktorError::new(format!("operation: {message}"), ()));
                            }
                            contain_drop(payload, &completion.kill, "local panic drop");
                        },
                    }
                    break;
                },
                changed = closed.changed(), if !stopping => {
                    if changed.is_err() || *closed.borrow_and_update() {
                        stopping = true;
                        alive.shutdown();
                    }
                },
                // Its grace is up, the calls go and `end` still gets its own grace.
                changed = forced.changed(), if !cancelling => {
                    if changed.is_err() || *forced.borrow_and_update() {
                        cancelling = true;
                        alive.cancel_calls();
                    }
                },
            }
        }

        report.ran_past_grace = retained.ran_past_grace();
        report.retirement = if retained.cleanup_completed() {
            crate::AktorRetireStatus::Confirmed
        } else {
            crate::AktorRetireStatus::Terminated
        };
        report.diagnostics.extend(retained.diagnostics());
        contain_drop(driver, &completion.kill, "local owner drop");
        contain_drop(alive, &completion.kill, "local handle drop");
        completion.publish(report);
    }));

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
        spawn(schedule(interval, handle.clone(), sleep));
    }

    Ok(handle)
}

fn schedule<S: 'static, Clock: 'static, C: 'static>(
    interval: LocalInterval<S>,
    handle: Handle<S, 0, C, (), Clock>,
    sleep: fn(Duration) -> LocalFuture<'static, ()>,
) -> LocalFuture<'static, ()> {
    let every = interval.every;
    let callback = interval.callback;

    Box::pin(async move {
        loop {
            tokio::select! {
                biased;
                _ = handle.wait_closing() => break,
                _ = sleep(every) => {},
            }

            let callback = callback.clone();
            let operation = Operation {
                name: "interval",
                caller: std::panic::Location::caller(),
            };
            let request = local::Request::new(
                &handle,
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
                _ = handle.wait_closing() => break,
                admitted = request.run_interval() => {
                    if !admitted { break; }
                },
            }
        }
    })
}
