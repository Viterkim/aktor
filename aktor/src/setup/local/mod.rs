use super::*;
use crate::setup::intervals::{LocalInterval, LocalIntervals};
use crate::{
    ActorFailure, ActorOutcome, AktorError, KillSwitch,
    group::shutdown::{contain_drop, panic_message},
    local::{self, Handle, hooks::AktorHooks},
    message::LocalFuture,
    operation::Operation,
};
use core::ops::AsyncFnOnce;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use tokio::sync::{oneshot, watch};

#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod tokio_local;

struct Completion {
    name: String,
    kind: AktorExecution,
    kill: KillSwitch,
    completed: Option<oneshot::Sender<ActorOutcome>>,
}
impl Completion {
    fn publish(&mut self, outcome: ActorOutcome) {
        if let Some(completed) = self.completed.take() {
            let _sent = completed.send(outcome);
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if self.completed.is_some() {
            self.kill.fail(ActorFailure {
                kind: Some(self.kind),
                actor: self.name.clone(),
                phase: "owner".into(),
                message: "local actor task cancelled".into(),
            });

            self.publish(ActorOutcome {
                kind: Some(self.kind),
                actor: self.name.clone(),
                diagnostics: vec![AktorError::new("local actor task cancelled")],
                timed_out: false,
            });
        }
    }
}

pub async fn start_local<S, Start, Kind, Clock>(
    setup: AktorSetup<S, Start, Kind>,
    mut context: AktorStartContext<crate::AktorGroup>,
    spawn: impl Fn(LocalFuture<'static, ()>),
    sleep: fn(Duration) -> LocalFuture<'static, ()>,
) -> Result<Handle<S, 0, (), (), Clock>, AktorStartError>
where
    S: 'static,
    Clock: 'static,
    Kind: AktorMode<
            Each<S> = dyn FnMut(&mut S, Operation),
            End<S> = dyn AktorEnd<S>,
            Interval<S> = dyn AktorIntervalLogic<S>,
        >,
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + 'static,
{
    let AktorSetup {
        name,
        role: _,
        kind: _,
        closures,
        options,
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
        return Err(AktorStartError::Setup(AktorError::new(
            "interval duration must be positive",
        )));
    }

    let LocalIntervals {
        scheduled: intervals,
        retained,
    } = LocalIntervals::new(intervals);
    let (handle, mut owner) = local::channel_with_clock::<S, 0, (), Clock>(options.capacity)
        .map_err(|error| AktorStartError::Setup(AktorError::new(error.to_string())))?;

    owner.hooks = AktorHooks {
        before_each: before_each.map(|hook| hook.0),
        after_each: after_each.map(|hook| hook.0),
        intervals: retained,
    };

    let kill = context.group.killswitch();

    handle.manage(name.clone(), kill.clone());

    let alive = handle.new_handle();
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
            Box::pin(async move {
                outcome.await.unwrap_or_else(|_| ActorOutcome {
                    kind: Some(Kind::EXECUTION),
                    actor: label,
                    diagnostics: vec![AktorError::new("local actor observer stopped")],
                    timed_out: false,
                })
            }),
        )
        .map_err(AktorStartError::Setup)?;

    let mut completion = Completion {
        name,
        kind: Kind::EXECUTION,
        kill: kill.clone(),
        completed: Some(completed),
    };

    spawn(Box::pin(async move {
        let retained = owner.completion();
        let mut driver = Box::pin(
            AssertUnwindSafe(owner.run_with_custom(
                async move || start().await,
                async move |state| {
                    if let Some(mut end) = end {
                        end.0.run(state).await
                    } else {
                        drop(state);
                        Ok(())
                    }
                },
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
        let mut report = ActorOutcome {
            kind: Some(Kind::EXECUTION),
            actor: completion.name.clone(),
            diagnostics: Vec::new(),
            timed_out: false,
        };

        loop {
            tokio::select! {
                biased;
                result = &mut driver => {
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => report.diagnostics.push(AktorError::new(error.to_string())),
                        Err(payload) => {
                            completion.kill.fail(ActorFailure {
                                kind: Some(Kind::EXECUTION),
                                actor: report.actor.clone(),
                                phase: "operation".into(),
                                message: panic_message(&payload),
                            });
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
                changed = forced.changed() => {
                    if changed.is_err() || *forced.borrow_and_update() {
                        report.timed_out = true;
                        break;
                    }
                },
            }
        }

        report.diagnostics.extend(retained.diagnostics());
        contain_drop(driver, &completion.kill, "local owner drop");
        contain_drop(alive, &completion.kill, "local handle drop");
        completion.publish(report);
    }));

    if let Err(error) = handle.ready().await {
        return Err(AktorStartError::Setup(AktorError::new(error.to_string())));
    }

    for interval in intervals {
        spawn(schedule(interval, handle.new_handle(), kill.clone(), sleep));
    }

    Ok(handle)
}

fn schedule<S: 'static, Clock: 'static>(
    interval: LocalInterval<S>,
    handle: Handle<S, 0, (), (), Clock>,
    kill: KillSwitch,
    sleep: fn(Duration) -> LocalFuture<'static, ()>,
) -> LocalFuture<'static, ()> {
    let every = interval.every;
    let callback = interval.callback;

    Box::pin(async move {
        loop {
            tokio::select! {
                biased;
                _ = kill.wait_stopping() => break,
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
                _ = kill.wait_stopping() => break,
                _ = handle.wait_closing() => break,
                admitted = request.run_interval() => {
                    if !admitted { break; }
                },
            }
        }
    })
}
