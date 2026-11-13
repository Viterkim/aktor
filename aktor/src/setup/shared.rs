use super::*;
use crate::{
    AktorError, AktorFailureReport, AktorGroup, AktorOutcome, AktorRetireStatus, cross_core,
    local::hooks::CallHooks,
    threads::{self, Thread},
};
use alloc::rc::Rc;
use core::{
    cell::{Cell, RefCell},
    ops::AsyncFnOnce,
};
use tokio::sync::oneshot;

impl<S: 'static, Start, End, CM: 'static, E, M: 'static> AktorStart
    for AktorNew<S, Start, kind::BrowserThread, End, (CM, E, M)>
where
    Start: AsyncFnOnce() -> Result<S, E> + Send + 'static,
    E: InitError<M> + 'static,
    E::Error: Send + 'static,
    End: closures::AktorEndCall<S, CM> + Send + 'static,
{
    type Group = AktorGroup;
    type Error = E::Error;
    type Handles = cross_core::Handle<S>;
    type Startup =
        crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError<Self::Error>>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorStartCause> {
        group
            .start()
            .map(|_| ())
            .map_err(AktorStartCause::group_error)
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let setup = self;

        Box::pin(async move {
            let name = setup.name;
            let stuck = setup.stuck;
            let kind = AktorExecution::BrowserThread;
            let fail = |error| {
                validation_error(&context.group, &name, kind, AktorStartCause::Setup(error))
            };

            let (handle, open_owner) =
                cross_core::browser_channel(setup.capacity).map_err(|error| {
                    AktorStartError::not_run(fail(AktorError::new(error.to_string(), ())))
                })?;

            if setup
                .hooks
                .intervals
                .iter()
                .any(|interval| interval.every.is_zero())
            {
                return Err(AktorStartError::not_run(fail(AktorError::new(
                    "interval duration must be positive",
                    (),
                ))));
            }

            let start = setup.start;
            let end = setup.end;
            let AktorHooks {
                intervals,
                before_each,
                after_each,
            } = setup.hooks;
            let (failed_start, mut init) = oneshot::channel();
            let (begin, accepted) = oneshot::channel();
            let kill = context.group.killswitch();
            let owner_kill = kill.clone();
            let label = name.clone();
            let completion = handle.completion();
            let owner_completion = completion.clone();

            handle.managed();
            let job = threads::insert(Box::new(move || {
                Box::pin(async move {
                    if accepted.await.is_err() {
                        return Ok(());
                    }

                    let mut owner = open_owner();
                    owner.hooks = CallHooks {
                        before_each: before_each.map(|hook| {
                            hook.0 as Box<dyn FnMut(&mut S, crate::operation::Operation)>
                        }),
                        after_each: after_each.map(|hook| {
                            hook.0 as Box<dyn FnMut(&mut S, crate::operation::Operation)>
                        }),
                        intervals: Vec::new(),
                    };
                    owner.set_intervals(
                        intervals
                            .into_iter()
                            .map(|interval| {
                                (
                                    interval.every,
                                    AktorClosure(interval.run.0 as Box<dyn AktorIntervalLogic<S>>),
                                )
                            })
                            .collect(),
                    )?;

                    let mut setup_failed = false;
                    let result = owner
                        .run_prepared(
                            || async {
                                start().await.map_err(|error| {
                                    let (data, report) = error.split();
                                    let mut unsent = None;
                                    setup_failed = true;

                                    let failure = owner_kill.startup_error_with(
                                        label.clone(),
                                        kind,
                                        AktorError::new(report, ()),
                                        |primary, diagnostics| {
                                            let failure = crate::AktorFailure {
                                                diagnostics: diagnostics.into(),
                                                primary,
                                            };
                                            unsent = failed_start.send((data, failure)).err();
                                        },
                                    );

                                    drop(unsent);
                                    failure
                                })
                            },
                            |state| async {
                                let mut result = end.run(state).await;

                                if let Err(error) = &mut result {
                                    error.primary = owner_kill.fail(AktorFailureReport {
                                        actor: label.clone(),
                                        kind: Some(kind),
                                        phase: "cleanup".into(),
                                        message: error.diagnostics.clone(),
                                    });
                                }

                                result
                            },
                            |_| {},
                        )
                        .await;

                    if setup_failed || owner_completion.cleanup_failed() {
                        Ok(())
                    } else {
                        result.map_err(|error| AktorError::new(error.diagnostics, ()))
                    }
                })
            }));
            let thread = match Thread::start(&setup.kind.program, job) {
                Ok(thread) => Rc::new(thread),
                Err(error) => {
                    threads::remove(job);
                    return Err(AktorStartError::not_run(fail(AktorError::new(
                        alloc::format!("{error:?}"),
                        (),
                    ))));
                }
            };

            let bootstrap_cancelled = Rc::new(Cell::new(false));
            let cancelled_bootstrap = bootstrap_cancelled.clone();
            let stopping_bootstrap = bootstrap_cancelled.clone();
            let shutdown = handle.clone();
            let killing = stuck == crate::AktorStuck::Kill;
            let cancelling_calls = killing.then(|| handle.clone());
            let failed = handle.clone();
            let startup_failure = Rc::new(RefCell::new(None));
            let monitor_startup = startup_failure.clone();
            let (finished, result) = oneshot::channel();
            let monitor = thread.clone();
            let monitor_kill = kill.clone();
            let label = name.clone();

            wasm_bindgen_futures::spawn_local(async move {
                let error = monitor
                    .wait()
                    .await
                    .err()
                    .map(|error| AktorError::new(alloc::format!("{error:?}"), ()));
                let never_entered = threads::remove(job) || cancelled_bootstrap.get();

                let mut secondary = None;
                if let Some(error) = error.filter(|_| !cancelled_bootstrap.get()) {
                    let initialized = failed.completion().initialized();
                    let failure = AktorFailureReport {
                        actor: label,
                        kind: Some(kind),
                        phase: if initialized { "owner" } else { "setup" }.into(),
                        message: error.diagnostics.clone(),
                    };

                    let primary = if initialized {
                        monitor_kill.fail(failure)
                    } else {
                        *monitor_startup.borrow_mut() = Some((error.clone(), never_entered));
                        monitor_kill.fail_startup(failure)
                    };

                    failed.finish_failed(error.clone());
                    if !primary {
                        secondary = Some(error);
                    }
                }

                let _sent =
                    finished.send((secondary, monitor.retirement_received(), never_entered));
            });

            let label = name.clone();
            let cancel = thread.clone();
            let bootstrap = thread.clone();

            let registered = context.group.register_owner(
                name.clone(),
                kind,
                Box::new(move || {
                    if threads::remove(job) {
                        stopping_bootstrap.set(true);
                        bootstrap.cancel();
                    }

                    shutdown.shutdown();
                }),
                Box::new(move || {
                    if let Some(handle) = cancelling_calls {
                        handle.cancel_calls();
                    }
                }),
                // Aborting the job doesn't stop the thread right away, so it isn't gone for sure.
                killing.then(|| {
                    Box::new(move || {
                        threads::remove(job);
                        cancel.cancel();
                        false
                    }) as Box<dyn FnOnce() -> bool>
                }),
                None,
                Box::pin(async move {
                    let result = result.await;
                    let (error, received, never_entered) = result.unwrap_or((None, false, false));
                    let cleaned = completion.cleanup_completed();
                    let mut diagnostics = completion.secondary_diagnostics();

                    if let Some(error) = error {
                        diagnostics.push(error);
                    }

                    AktorOutcome {
                        retirement: match (received, cleaned || never_entered) {
                            (true, true) => AktorRetireStatus::Confirmed,
                            (true, false) => AktorRetireStatus::Terminated,
                            (false, _) => AktorRetireStatus::Unconfirmed,
                        },
                        actor: label,
                        kind,
                        diagnostics,
                        ran_past_grace: completion.cut_call(),
                    }
                }),
            );

            if let Err(error) = registered {
                bootstrap_cancelled.set(true);
                threads::remove(job);
                thread.cancel();
                return Err(AktorStartError::not_run(AktorStartCause::Group(error)));
            }
            let _sent = begin.send(());

            let startup_error = || match startup_failure.borrow_mut().take() {
                Some((error, true)) => AktorStartError::not_run(AktorStartCause::Setup(error)),
                Some((error, false)) => AktorStartCause::Setup(error).into(),
                None if bootstrap_cancelled.get() => {
                    AktorStartError::not_run(AktorStartCause::Group(AktorGroupError::Stopping))
                }
                None => AktorGroupError::Stopping.into(),
            };
            let completed = context.group.completion();
            let started = tokio::select! {
                biased;
                ready = handle.ready() => Some(ready),
                _ = completed.wait() => None,
            };

            if let Ok((data, failure)) = init.try_recv() {
                return Err(AktorStartError {
                    init: AktorInitOutcome::Failed(data),
                    cause: AktorStartCause::Init(failure),
                    report: None,
                });
            }

            match started {
                Some(Ok(())) => {}
                Some(Err((_, report))) if !kill.is_stopping() => {
                    return Err(validation_error(
                        &context.group,
                        &name,
                        kind,
                        AktorStartCause::Setup(AktorError::new(report, ())),
                    )
                    .into());
                }
                Some(Err(_)) | None => return Err(startup_error()),
            }

            // The group lives on this thread, `handle.run(work)` hears about it closing here.
            let watching = handle.downgrade();

            let ending = thread.clone();

            // An actor that ends closes its group too, so the watcher ends with either.
            wasm_bindgen_futures::spawn_local(async move {
                let stopping = core::pin::pin!(kill.wait_stopping());
                let ended = core::pin::pin!(ending.wait());

                let _ = futures_util::future::select(stopping, ended).await;
                watching.group_stopped();
            });

            Ok(handle)
        })
    }
}
