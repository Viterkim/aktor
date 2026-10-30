use super::*;
use crate::{
    AktorError, AktorFailureReport, AktorGroup, AktorOutcome, cross_core,
    local::hooks::AktorHooks,
    threads::{self, Thread},
};
use alloc::rc::Rc;
use core::{
    cell::{Cell, RefCell},
    ops::AsyncFnOnce,
};
use tokio::sync::oneshot;

impl<S: 'static, Start, Role, End, CM> AktorStart<CM>
    for AktorNew<S, Start, kind::BrowserThread, Role, End>
where
    Start: AsyncFnOnce() -> Result<S, AktorSetupError> + Send + 'static,
    End: closures::AktorEndCall<S, CM> + Send + 'static,
{
    type Group = AktorGroup;
    type Handles = cross_core::Handle<S, Role>;
    type Startup = crate::message::LocalFuture<'static, Result<Self::Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        group.start().map(|_| ()).map_err(Into::into)
    }

    fn start_in(self, mut context: AktorStartContext<Self::Group>) -> Self::Startup {
        let setup = self.with_role(AktorNoRole);

        Box::pin(async move {
            let name = setup.name.name;
            let kind = AktorExecution::BrowserThread;
            let fail = |error| {
                validation_error(&context.group, &name, kind, AktorStartError::Setup(error))
            };

            if setup.options.capacity == 0 {
                return Err(fail(AktorError::new("actor capacity must be positive", ())));
            }

            if setup
                .closures
                .intervals
                .iter()
                .any(|interval| interval.every.is_zero())
            {
                return Err(fail(AktorError::new(
                    "interval duration must be positive",
                    (),
                )));
            }

            let AktorClosures {
                start,
                mut end,
                intervals,
                before_each,
                after_each,
            } = setup.closures;
            let (opened, handle) = oneshot::channel();
            let capacity = setup.options.capacity;
            let job = threads::insert(Box::new(move || {
                Box::pin(async move {
                    let (handle, mut owner) = cross_core::channel(capacity)
                        .map_err(|error| AktorError::new(error.to_string(), ()))?;

                    handle.managed();
                    owner.hooks = AktorHooks {
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

                    if opened.send(handle).is_err() {
                        return Err(AktorError::new("shared actor startup was cancelled", ()));
                    }

                    owner
                        .run_prepared(
                            async move || start().await.map_err(Into::into),
                            async move |state| end.run(state).await,
                        )
                        .await
                        .map_err(|error| AktorError::new(error.diagnostics, ()))
                })
            }));
            let thread = match Thread::start(&setup.kind.program, job) {
                Ok(thread) => Rc::new(thread),
                Err(error) => {
                    threads::remove(job);
                    return Err(fail(AktorError::new(alloc::format!("{error:?}"), ())));
                }
            };

            let stopping = Rc::new(Cell::new(false));
            let stopped = stopping.clone();
            let retained = Rc::new(RefCell::new(None::<cross_core::Handle<S>>));
            let shutdown = retained.clone();
            let diagnostics = retained.clone();
            let failed = retained.clone();
            let initialized = Rc::new(Cell::new(false));
            let ready = initialized.clone();
            let (finished, result) = oneshot::channel();
            let monitor = thread.clone();
            let kill = context.group.killswitch();
            let label = name.clone();

            wasm_bindgen_futures::spawn_local(async move {
                let error = monitor
                    .wait()
                    .await
                    .err()
                    .map(|error| AktorError::new(alloc::format!("{error:?}"), ()));
                threads::remove(job);

                if let Some(error) = &error {
                    let failure = AktorFailureReport {
                        actor: label,
                        kind: Some(kind),
                        phase: if initialized.get() { "owner" } else { "setup" }.into(),
                        message: error.diagnostics.clone(),
                    };

                    if initialized.get() {
                        kill.fail(failure);
                    } else {
                        kill.fail_startup(failure);
                    }

                    if let Some(handle) = &*failed.borrow() {
                        handle.finish_failed(error.clone());
                    }
                }

                let _sent = finished.send((error, monitor.retirement_received()));
            });

            let cancelled = Rc::new(Cell::new(false));
            let forced = cancelled.clone();
            let label = name.clone();
            let cancel = thread.clone();
            let bootstrap = thread.clone();

            let registered = context.group.register_owner(
                name.clone(),
                kind,
                Box::new(move || {
                    stopped.set(true);

                    if let Some(handle) = &*shutdown.borrow() {
                        handle.shutdown();
                    } else if threads::remove(job) {
                        bootstrap.cancel();
                    }
                }),
                Box::new(move || {
                    forced.set(true);
                    threads::remove(job);
                    cancel.cancel();
                }),
                Box::pin(async move {
                    let result = result.await;
                    let (error, received) = result.unwrap_or((None, false));
                    let confirmed = received
                        && diagnostics
                            .borrow()
                            .as_ref()
                            .is_some_and(|handle| handle.completion().cleanup_completed());
                    let mut diagnostics = diagnostics
                        .borrow()
                        .as_ref()
                        .map(|handle| handle.completion().diagnostics())
                        .unwrap_or_default();

                    if let Some(error) = error {
                        diagnostics.push(error);
                    }

                    AktorOutcome {
                        retirement: if !confirmed {
                            crate::RetirementStatus::Unconfirmed
                        } else {
                            crate::RetirementStatus::Confirmed
                        },
                        actor: label,
                        kind: Some(kind),
                        diagnostics,
                        timed_out: cancelled.get(),
                    }
                }),
            );

            if let Err(error) = registered {
                threads::remove(job);
                thread.cancel();
                return Err(validation_error(
                    &context.group,
                    &name,
                    kind,
                    AktorStartError::Setup(error),
                ));
            }

            let kill = context.group.killswitch();
            let handle = tokio::select! {
                value = handle => value.map_err(|_| validation_error(&context.group, &name, kind,
                    AktorStartError::Setup(AktorError::new("shared actor stopped during startup", ()))) )?,
                _ = kill.wait_stopping() => return Err(AktorStartError::Setup(AktorError::new("shared actor stopped during startup", ()))),
            };

            *retained.borrow_mut() = Some(handle.new_handle());

            if stopping.get() {
                handle.shutdown();
            }

            tokio::select! {
                biased;
                ready = handle.ready() => ready.map_err(|error| {
                    AktorStartError::Init(kill.startup_error(name.clone(), kind, error))
                })?,
                _ = kill.wait_stopping() => return Err(AktorStartError::Setup(AktorError::new("shared actor stopped during startup", ()))),
            }

            ready.set(true);
            Ok(handle.with_role::<Role>())
        })
    }
}
