use super::*;
use crate::{
    AktorFailureInput, AktorGroup, AktorRetirement, AktorStuck, OwnerOutcome, cross_core,
    local::hooks::CallHooks,
    threads::{self, Thread},
};
use alloc::rc::Rc;
use core::{
    cell::{Cell, RefCell},
    ops::AsyncFnOnce,
};
use tokio::sync::oneshot;

#[derive(Er)]
enum BrowserThreadErr {
    Open,
    Exited,
    Destroy,
}
impl<S: 'static, Start, End, EM: 'static, E, X, Before, After> AktorStart
    for AktorNew<S, AktorInit<Start>, kind::BrowserThread, End, (EM, E), Before, After>
where
    Before:
        crate::setup::closures::EachHook<<kind::BrowserThread as crate::setup::AktorMode>::Each<S>>,
    After:
        crate::setup::closures::EachHook<<kind::BrowserThread as crate::setup::AktorMode>::Each<S>>,
    Start: AsyncFnOnce() -> Result<S, X> + Send + 'static,
    X: Into<(E, String)> + 'static,
    E: Send + 'static,
    End: closures::AktorEndCall<S, EM> + Send + 'static,
{
    type Group = AktorGroup;
    type Error = E;
    type Handles = cross_core::Handle<S>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), (AktorStartReason, String)> {
        group.start().er_with(|error| error.start_reason())?;

        Ok(())
    }

    fn actor_name(&self) -> &str {
        &self.name
    }

    fn closed(_name: &str, group: &Self::Group) -> Self::Handles {
        let (handle, owner) = cross_core::closed_channel(group.killswitch().registered_id());
        drop(owner);
        handle
    }

    fn launch(self, mut context: AktorStartContext<Self::Group>) -> AktorLaunch<Self> {
        let this = pair_start!(self, crate::setup::impls::pair_start, EM);

        let setup = this;
        let name = setup.name;
        let stuck = setup.stuck;
        let kind = AktorExecution::BrowserThread;
        let invalid = || LaunchError::new(AktorStartReason::InvalidSetup);
        let fail = |error| validation_error(&context.group, &name, kind, error);
        let (handle, open_owner) = cross_core::browser_channel(setup.capacity);

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

        handle.manage(kill.actor_id());
        let job = threads::insert(Box::new(move || {
            Box::pin(async move {
                if accepted.await.is_err() {
                    return Ok(());
                }

                let mut owner = open_owner();
                owner.manage_thread(label.clone(), owner_kill.clone());
                owner.hooks = CallHooks {
                    before_each: before_each
                        .map(|hook| hook as Box<dyn FnMut(&mut S, crate::operation::Operation)>),
                    after_each: after_each
                        .map(|hook| hook as Box<dyn FnMut(&mut S, crate::operation::Operation)>),
                    intervals: Vec::new(),
                };
                for interval in intervals {
                    owner.add_interval(
                        interval.every,
                        AktorClosure(interval.run.0 as Box<dyn AktorIntervalLogic<S>>),
                    );
                }

                let mut setup_failed = false;
                let result = owner
                    .run_prepared(
                        || async {
                            let error = match start().await {
                                Ok(state) => {
                                    if owner_kill.is_startup_aborted() {
                                        let failure = er_new!(
                                            LaunchError::new(AktorStartReason::Stopping),
                                            AktorGroupError::Stopping
                                        );
                                        let _sent = failed_start.send(failure.into());
                                    }
                                    return Ok(state);
                                }
                                Err(error) => error,
                            };

                            let mut failure = LaunchError::init(error);
                            let message = failure.1.clone();
                            let mut unsent = None;
                            setup_failed = true;

                            let primary = owner_kill.startup_error_with(
                                label.clone(),
                                kind,
                                message.clone(),
                                |id| {
                                    failure.0.failure_id = Some(id);
                                    unsent = failed_start.send(failure).err();
                                },
                            );

                            drop(unsent);
                            Err((cross_core::RunErr { id: Some(primary) }, message))
                        },
                        |state| async {
                            let result = match stuck {
                                AktorStuck::Leave => end.run(state).await,
                                AktorStuck::Kill => {
                                    owner_kill.end_within(end.run(state), false).await?
                                }
                            };
                            Some(match result {
                                Ok(()) => Ok(()),
                                Err((_, report)) => {
                                    let primary = owner_kill.fail(AktorFailureInput::actor(
                                        label.clone(),
                                        kind,
                                        crate::AktorPhase::End,
                                        report.clone(),
                                    ));

                                    Err((cross_core::RunErr { id: Some(primary) }, report))
                                }
                            })
                        },
                    )
                    .await;

                // These already reached the report, the job itself didn't fail.
                if setup_failed || owner_completion.cleanup_failed() || owner_completion.ran_out() {
                    Ok(())
                } else {
                    Ok(result.er(|| cross_core::OwnerError::Completion)?)
                }
            })
        }));
        let thread = match Thread::start(&setup.kind.program, job) {
            Ok(thread) => Rc::new(thread),
            Err(error) => {
                threads::remove(job);
                let error = er_new!(
                    invalid(),
                    BrowserThreadErr::Open.er_save("browser", || js_sys::Error::from(error)
                        .message()
                        .as_string()
                        .unwrap_or_default())
                );
                return Err(fail(error.into()));
            }
        };

        let bootstrap_cancelled = Rc::new(Cell::new(false));
        let cancelled_bootstrap = bootstrap_cancelled.clone();
        let stopping_bootstrap = bootstrap_cancelled.clone();
        let control = handle.control();
        let shutdown = control.clone();
        let killing = stuck == crate::AktorStuck::Kill;
        let cancelling_calls = killing.then(|| control.clone());
        let failed = control;
        let startup_failure = Rc::new(RefCell::new(None));
        let monitor_startup = startup_failure.clone();
        let (finished, result) = oneshot::channel();
        let monitor = thread.clone();
        let monitor_kill = kill.clone();
        let label = name.clone();

        wasm_bindgen_futures::spawn_local(async move {
            let error = monitor.wait().await.err().map(|error| {
                BrowserThreadErr::Exited
                    .er_save("browser", || error.as_string().unwrap_or_default())
                    .er_report()
            });
            let never_entered = threads::remove(job) || cancelled_bootstrap.get();

            if let Some(error) = error.filter(|_| !cancelled_bootstrap.get()) {
                let initialized = failed.completion().initialized();
                let failure = AktorFailureInput::actor(
                    label.clone(),
                    kind,
                    failed.completion().interrupted_phase(),
                    error.clone(),
                );

                if !initialized {
                    *monitor_startup.borrow_mut() = Some(error.clone());
                }
                let receipt = monitor_kill.record_failure(failure, !initialized, |_| {});

                failed.finish_failed(error, receipt);
            }

            if let Some(error) = monitor.destruction_error() {
                let report = BrowserThreadErr::Destroy
                    .er_save("browser", error)
                    .er_report();
                let receipt = monitor_kill.record_failure(
                    AktorFailureInput::actor(label, kind, crate::AktorPhase::Drop, report.clone()),
                    false,
                    |_| {},
                );
                if !failed.completion().initialized() && monitor_startup.borrow().is_none() {
                    *monitor_startup.borrow_mut() = Some(report.clone());
                }
                failed.finish_failed(report, receipt);
            }
            let _sent = finished.send((monitor.retirement_received(), never_entered));
        });

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
                let (received, never_entered) = result.unwrap_or((false, false));
                let cleaned = completion.cleanup_completed();
                OwnerOutcome {
                    retirement: match (received, cleaned || never_entered) {
                        (true, true) => AktorRetirement::Released,
                        (true, false) => AktorRetirement::Terminated,
                        (false, _) => AktorRetirement::StillRunning,
                    },
                    init: if never_entered {
                        crate::AktorInitEvidence::NeverEntered
                    } else {
                        completion.init()
                    },
                    ran_past_grace: completion.cut_call(),
                }
            }),
        );

        if let Err(error) = registered {
            bootstrap_cancelled.set(true);
            threads::remove(job);
            thread.cancel();
            er_bail!(LaunchError::new(error.0.start_reason()), error);
        }
        let _sent = begin.send(());

        wasm_bindgen_futures::spawn_local(crate::group::keeper(
            handle.clone(),
            context.group.completion(),
        ));

        // The group lives on this thread, `handle.run(work)` hears about it closing here.
        let watching = handle.downgrade();

        let ending = thread.clone();
        let watch_kill = kill.clone();

        // An actor that ends closes its group too, so the watcher ends with either.
        wasm_bindgen_futures::spawn_local(async move {
            let stopping = core::pin::pin!(watch_kill.wait_stopping());
            let ended = core::pin::pin!(ending.wait());

            let _ = futures_util::future::select(stopping, ended).await;
            watching.group_stopped();
        });

        let waiting = handle.clone();
        let group = context.group;
        let ready: crate::message::LocalFuture<'static, _> = Box::pin(async move {
            let context = AktorStartContext { group };
            let handle = waiting;

            let completed = context.group.completion();
            let started = tokio::select! {
                biased;
                ready = handle.observe_ready() => Some(ready),
                _ = completed.wait() => None,
            };

            if let Ok(failure) = init.try_recv() {
                return Err(failure);
            }

            match started {
                Some(Ok(())) => {}
                Some(Err(failure)) if !kill.is_stopping() => {
                    let error = er_new!(invalid(), failure);
                    return Err(validation_error(&context.group, &name, kind, error.into()));
                }
                Some(Err(_)) | None => match startup_failure.borrow_mut().take() {
                    Some(report) => er_bail!(invalid().er_save("thread", || report)),
                    None => er_bail!(
                        LaunchError::new(AktorStartReason::Stopping),
                        AktorGroupError::Stopping
                    ),
                },
            }

            Ok(())
        });

        Ok((handle, ready))
    }
}
