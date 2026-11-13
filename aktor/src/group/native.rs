use super::*;
use crate::AktorShutdownPolicy;
#[cfg(any(feature = "tokio", feature = "std_thread"))]
use crate::{
    Aktor, AktorError,
    listener::{DedicatedJoinError, DedicatedStartError, FailurePolicy, SpawnArgs, StartupJoin},
    owner::{OwnerError, StartError},
};
#[cfg(any(feature = "tokio", feature = "std_thread"))]
use crate::{AktorCleanupOutput, AktorExecution, AktorFailure};
use std::collections::HashMap;

pub fn watchdog(kill: &AktorKillSwitch) {
    let worker = kill.clone();
    let watchdog = std::thread::Builder::new()
        .name("aktor shutdown".into())
        .spawn(move || {
            let control = &worker.control;
            let mut state = control.lock();

            loop {
                if settled(&state)
                    || (control.shutdown_policy == AktorShutdownPolicy::KeepAlive
                        && state.finished
                        && !state.callback_running)
                {
                    state.watchdog_disposal = WatchdogDisposal::Gone;
                    return;
                }

                if state.watchdog_disposal == WatchdogDisposal::Requested {
                    let report = state.pending_report();
                    state.watchdog_disposal = WatchdogDisposal::Gone;
                    drop(state);

                    // This drop can block the watchdog too. Publish the report first.
                    control.publish_pending(report);
                    worker.drop_shutdown_hooks();
                    return;
                }

                // Shutdown pulls the deadline in once every actor had its turn.
                let deadline = state.deadline.unwrap_or_else(Instant::now);
                let remaining = deadline.saturating_duration_since(Instant::now());

                if remaining.is_zero() {
                    if control.shutdown_policy == AktorShutdownPolicy::AbortProcess {
                        drop(state);
                        terminate();
                    }

                    let callback = state.callback_running;
                    if callback {
                        state.report.callback_timed_out();
                    } else {
                        state.report.ran_past_grace = true;
                    }
                    let report = state.pending_report();
                    state.watchdog_disposal = WatchdogDisposal::Gone;
                    drop(state);
                    control.publish_pending(report);
                    return;
                }

                let (next, _) = control
                    .wake
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|error| error.into_inner());

                state = next;
            }
        });

    if let Err(error) = watchdog {
        if kill.control.shutdown_policy == AktorShutdownPolicy::AbortProcess {
            terminate();
        }

        let (report, abandoned) = {
            let mut state = kill.control.lock();
            state.report.application.push(crate::AktorError::new(
                format!("Could not start the shutdown watchdog: {error}"),
                (),
            ));
            let abandoned = state.watchdog_disposal == WatchdogDisposal::Requested;
            state.watchdog_disposal = WatchdogDisposal::Gone;
            if abandoned {
                state.abandon_shutdown_hooks();
            }

            (state.pending_report(), abandoned)
        };

        if abandoned {
            kill.publish_stopped();
        } else {
            kill.control.publish_pending(report);
        }
        return;
    }

    if kill.control.shutdown_policy == AktorShutdownPolicy::KeepAlive {
        return;
    }

    let control = kill.control.clone();
    let reserve = super::impls::reserve(kill.control.grace);
    let _ = std::thread::Builder::new()
        .name("aktor shutdown report".into())
        .spawn(move || {
            let (report, application_work, callback_running) = {
                let mut state = control.lock();

                loop {
                    if settled(&state) {
                        return;
                    }

                    let deadline = state.deadline.unwrap_or_else(Instant::now);
                    let reporting_at = deadline.checked_sub(reserve / 8).unwrap_or(deadline);
                    let remaining = reporting_at.saturating_duration_since(Instant::now());

                    if remaining.is_zero() {
                        break (
                            state.pending_report(),
                            state.applications != 0,
                            state.callback_running,
                        );
                    }

                    let (next, _) = control
                        .wake
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|error| error.into_inner());

                    state = next;
                }
            };

            let mut details = report.to_string();
            if application_work {
                details.push_str("Waiting for application work\n");
            }
            if callback_running {
                details.push_str("Waiting for the shutdown callback\n");
            }
            if details.is_empty() {
                details.push_str("Waiting for the group to finish shutdown\n");
            }
            eprint!("Shutdown still pending:\n{details}");
        });
}

fn settled(state: &State) -> bool {
    (state.finished || state.actors_and_hooks_done)
        && !state.callback_running
        && state.applications == 0
        && state.only_left()
}

impl Control {
    fn publish_pending(&self, report: AktorShutdownReport) {
        self.completed.send_if_modified(|completed| {
            if completed.is_some() {
                return false;
            }

            *completed = Some(Completed {
                report,
                retirement_final: false,
            });
            true
        });
    }
}

fn terminate() -> ! {
    #[cfg(unix)]
    {
        use rustix::process::{Signal, getpid, kill_process};

        // Even a stuck abort handler cannot catch SIGKILL.
        let _ = kill_process(getpid(), Signal::KILL);
    }
    std::process::abort();
}

impl AktorKillSwitch {
    #[doc(hidden)]
    pub fn track_thread(&self, name: String, stuck: crate::AktorStuck) -> ThreadLife {
        let name = Arc::new(name);

        self.control.lock().running.push((name.clone(), stuck));
        ThreadLife {
            control: self.control.clone(),
            name,
        }
    }
}

impl State {
    pub(super) fn abandon_shutdown_hooks(&mut self) {
        // Both shutdown threads are unavailable. The caller cannot safely run this destructor.
        std::mem::forget(self.listener_hook.take());
        std::mem::forget(self.shutdown_hook.take());
        self.callback_running = false;
        self.report.application.push(crate::AktorError::new(
            "The cancelled shutdown closure could not be dropped",
            (),
        ));
    }

    fn pending_report(&self) -> AktorShutdownReport {
        let mut report = self.report.clone();
        if report.actors.len() == self.kinds.len() {
            return report;
        }

        let mut reported = HashMap::<_, usize>::new();
        for actor in &self.report.actors {
            *reported.entry((&actor.actor, actor.kind)).or_default() += 1;
        }

        for (name, kind) in &self.kinds {
            if let Some(count) = reported.get_mut(&(name, *kind))
                && *count > 0
            {
                *count -= 1;
                continue;
            }

            report.actors.push(AktorOutcome {
                actor: name.clone(),
                kind: *kind,
                retirement: crate::AktorRetireStatus::Unconfirmed,
                diagnostics: Vec::new(),
                ran_past_grace: false,
            });
        }
        report
    }

    /// Every thread still running belongs to an actor shutdown may leave behind.
    pub(super) fn only_left(&self) -> bool {
        self.running
            .iter()
            .all(|(_, stuck)| *stuck == crate::AktorStuck::Leave)
    }
}

impl Drop for ThreadLife {
    fn drop(&mut self) {
        let mut state = self.control.lock();

        state
            .running
            .retain(|(name, _)| !Arc::ptr_eq(name, &self.name));

        drop(state);
        self.control.wake.notify_all();
        self.control.threads.notify_one();
    }
}

#[cfg(any(feature = "tokio", feature = "std_thread"))]
impl AktorGroup {
    #[doc(hidden)]
    /// Move an existing value into an actor. Cleanup just drops it.
    pub async fn spawn_value<S: Send + 'static>(
        &mut self,
        name: impl Into<String>,
        value: S,
    ) -> Result<Aktor<S>, (StartError, String)> {
        let setup = move || Ok::<_, String>(value);
        let cleanup = |_| Ok::<_, String>(());
        self.spawn_args(AktorArgs::new(name, setup, cleanup)).await
    }

    /// Start an actor in this group.
    #[doc(hidden)]
    pub async fn spawn_args<S, Setup, Cleanup, Output>(
        &mut self,
        args: AktorArgs<Setup, Cleanup>,
    ) -> Result<Aktor<S>, (StartError, String)>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> Result<S, String> + Send + 'static,
        Cleanup: FnMut(S) -> Output + Send + 'static,
    {
        let AktorArgs {
            name,
            capacity,
            setup,
            mut cleanup,
        } = args;

        self.spawn_async(AktorArgs {
            name,
            capacity,
            setup: move || core::future::ready(setup()),
            cleanup: move |state| core::future::ready(cleanup(state)),
        })
        .await
    }

    #[doc(hidden)]
    pub async fn spawn_async<S, Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        &mut self,
        args: AktorArgs<Setup, Cleanup>,
    ) -> Result<Aktor<S>, (StartError, String)>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, String>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        self.spawn_async_with_hooks(args, crate::listener::hooks::CallHooks::default())
            .await
    }

    #[doc(hidden)]
    pub async fn spawn_async_with_hooks<S, Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        &mut self,
        args: AktorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::CallHooks<S>,
    ) -> Result<Aktor<S>, (StartError, String)>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, String>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        #[cfg(feature = "std_thread")]
        let standard = self
            .control
            .standard
            .load(std::sync::atomic::Ordering::Acquire);
        #[cfg(not(feature = "std_thread"))]
        let standard = false;

        self.spawn_async_on(args, hooks, standard).await
    }

    #[doc(hidden)]
    pub async fn spawn_async_on<S, Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        &mut self,
        args: AktorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::CallHooks<S>,
        standard: bool,
    ) -> Result<Aktor<S>, (StartError, String)>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, String>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        let AktorArgs {
            name,
            capacity,
            setup,
            mut cleanup,
        } = args;
        let kill = self.killswitch();
        let actor = name.clone();
        let kind = if standard {
            AktorExecution::StdThread
        } else {
            AktorExecution::TokioThread
        };

        self.spawn_async_initial_on_reported::<S, _, _, _, _, _>(
            AktorArgs {
                name,
                capacity,
                setup: move || async move {
                    match setup().await {
                        Ok(state) => Ok(state),
                        Err(report) => {
                            Err(kill.startup_error(actor, kind, crate::AktorError::new(report, ())))
                        }
                    }
                },
                cleanup: move |state| {
                    let future = cleanup(state);
                    async move {
                        AktorCleanupOutput::into_cleanup(future.await).map_err(AktorFailure::from)
                    }
                },
            },
            hooks,
            standard,
            crate::AktorStuck::Kill,
            |error| (error.diagnostics.clone(), error.primary),
        )
        .await
        .map_err(|error| {
            let error = StartError::from(error);
            let report = match self.completion().try_report() {
                Some(report) if report.failed() => report.to_string(),
                _ => error.report(),
            };

            (error, report)
        })
    }

    #[doc(hidden)]
    pub async fn spawn_async_initial_on_reported<
        S,
        Initial,
        Setup,
        SetupFuture,
        Cleanup,
        CleanupFuture,
    >(
        &mut self,
        args: AktorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::CallHooks<S>,
        standard: bool,
        stuck: crate::AktorStuck,
        describe_initial: fn(&Initial) -> (String, bool),
    ) -> Result<Aktor<S>, DedicatedStartError<Initial>>
    where
        S: 'static,
        Initial: Send + 'static,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, Initial>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), AktorFailure>>,
    {
        let kind = if standard {
            AktorExecution::StdThread
        } else {
            AktorExecution::TokioThread
        };

        let AktorArgs {
            name,
            capacity,
            setup,
            mut cleanup,
        } = args;

        let kill = self.killswitch();

        if kill.is_stopping() {
            self.completion().wait().await;
            return Err(DedicatedStartError::Closed);
        }

        if !self.control.lock().listening {
            return Err(DedicatedStartError::NotStarted);
        }

        let runtime = match self.spawner(standard) {
            Ok(runtime) => runtime,
            Err(_) => {
                let error = DedicatedStartError::NoRuntime;

                kill.fail_startup(AktorFailureReport {
                    kind: None,
                    actor: name,
                    phase: "setup".into(),
                    message: error.to_string(),
                });

                self.completion().wait().await;
                return Err(error);
            }
        };

        let (started, ready) = tokio::sync::oneshot::channel();
        let (complete, mut outcome) = tokio::sync::mpsc::unbounded_channel();
        let completed = complete.clone();
        let (closing, mut closed) = watch::channel(false);
        let (forcing, mut forced) = watch::channel(false);
        let label = name.clone();

        // Keep startup owned even if the application stops awaiting it.
        if let Err(error) = self.register_owner(
            name.clone(),
            kind,
            Box::new(move || {
                closing.send_replace(true);
            }),
            Box::new(move || {
                forcing.send_replace(true);
            }),
            None,
            None,
            Box::pin(async move {
                outcome.recv().await.unwrap_or_else(|| AktorOutcome {
                    retirement: crate::AktorRetireStatus::Unconfirmed,
                    kind,
                    actor: label,
                    diagnostics: vec![AktorError::new("startup observer stopped", ())],
                    ran_past_grace: false,
                })
            }),
        ) {
            if matches!(error, crate::AktorGroupError::NotStarted) {
                return Err(DedicatedStartError::NotStarted);
            }
            self.completion().wait().await;
            return Err(DedicatedStartError::Closed);
        }

        let actor_name = name.clone();
        let cleanup_name = name.clone();
        let cleanup_kill = kill.clone();

        let scheduled = runtime.spawn_named("aktor supervisor", async move {
            let (owner, observing) = match Aktor::<S>::spawn_observed(
                SpawnArgs {
                    name: name.clone(),
                    capacity,
                    failure: FailurePolicy::Group(kill.clone()),
                    setup,
                    cleanup: move |state| {
                        let future = cleanup(state);
                        let actor = cleanup_name.clone();
                        let kill = cleanup_kill.clone();

                        async move {
                            let mut result = future.await;

                            if let Err(error) = &mut result {
                                error.primary = kill.fail(AktorFailureReport {
                                    actor,
                                    kind: Some(kind),
                                    phase: "cleanup".into(),
                                    message: error.diagnostics.clone(),
                                });
                            }

                            result
                        }
                    },
                },
                hooks,
                standard,
                Some(forced.clone()),
                stuck,
            )
            .await
            {
                Ok(owner) => owner,
                Err(error) => {
                    let join_failed = matches!(
                        &error,
                        DedicatedStartError::Startup {
                            join: StartupJoin::Failed(_),
                            ..
                        }
                    );
                    let interrupted = match error.thread_end() {
                        crate::owner::AktorThreadEnd::Joined => crate::AktorRetireStatus::Terminated,
                        crate::owner::AktorThreadEnd::NeverCreated => crate::AktorRetireStatus::Confirmed,
                        crate::owner::AktorThreadEnd::Unconfirmed => crate::AktorRetireStatus::Unconfirmed,
                    };
                    if !join_failed
                        && matches!(error.primary(), DedicatedStartError::Closed)
                        && kill.is_stopping()
                    {
                        kill.control.lock().report.ran_past_grace = true;

                        let sent = started.send(Err(error));
                        super::shutdown::contain_drop(sent, &kill, "startup reply drop");
                        let _sent = completed.send(AktorOutcome {
                            retirement: interrupted,
                            kind,
                            actor: name,
                            diagnostics: Vec::new(),
                            ran_past_grace: true,
                        });

                        return;
                    }

                    let (diagnostics, primary) = match error.primary() {
                        DedicatedStartError::Startup { .. } => (error.to_string(), false),
                        DedicatedStartError::Observer { cause, .. } => (cause.to_string(), false),
                        DedicatedStartError::Init(error) => describe_initial(error),
                        DedicatedStartError::NotStarted => {
                            ("actor group has not started".into(), false)
                        }
                        DedicatedStartError::Closed => {
                            ("actor group closed during startup".into(), false)
                        }
                        DedicatedStartError::NoRuntime => ("no actor runtime".into(), false),
                        DedicatedStartError::InvalidCapacity => {
                            ("invalid actor capacity".into(), false)
                        }
                        DedicatedStartError::ThreadNotCreated(error)
                        | DedicatedStartError::Thread(error) => (error.to_string(), false),
                        DedicatedStartError::Panicked { actor, cause } => {
                            (format!("actor {actor} startup panicked: {cause}"), false)
                        }
                    };

                    let join_diagnostics = if let DedicatedStartError::Startup {
                        join: StartupJoin::Failed(cause),
                        ..
                    } = &error
                    {
                        Some(format!("Startup join failed: {cause}"))
                    } else {
                        None
                    };
                    let first = !primary && kill.fail_startup(AktorFailureReport {
                        kind: Some(kind),
                        actor: name.clone(),
                        phase: "setup".into(),
                        message: diagnostics.clone(),
                    });

                    let retirement = if join_failed {
                        interrupted
                    } else {
                        match error.primary() {
                            DedicatedStartError::Startup { .. }
                            | DedicatedStartError::Observer { .. }
                            | DedicatedStartError::Panicked { .. }
                            | DedicatedStartError::Closed => interrupted,
                            DedicatedStartError::Init(_)
                            | DedicatedStartError::NotStarted
                            | DedicatedStartError::NoRuntime
                            | DedicatedStartError::InvalidCapacity
                            | DedicatedStartError::ThreadNotCreated(_)
                            | DedicatedStartError::Thread(_) => crate::AktorRetireStatus::Confirmed,
                        }
                    };
                    let diagnostics = (!first && !primary)
                        .then_some(diagnostics)
                        .into_iter()
                        .chain(join_diagnostics)
                        .map(|message| AktorError::new(message, ()))
                        .collect();
                    let sent = started.send(Err(error));
                    super::shutdown::contain_drop(sent, &kill, "startup reply drop");
                    let _sent = completed.send(AktorOutcome {
                        retirement,
                        kind,
                        actor: name,
                        diagnostics,
                        ran_past_grace: false,
                    });

                    return;
                }
            };

            let mut observing = core::pin::pin!(observing);
            let mut joined = false;
            let actor = owner.control.clone();
            let completion = owner.completion();
            let alive = owner.clone();
            let _sent = started.send(Ok(owner));
            let mut stopping = false;
            let mut cancelling = false;

            let result = loop {
                if !stopping && *closed.borrow_and_update() {
                    stopping = true;
                    super::shutdown::contain(|| actor.request_shutdown(), &kill, "shutdown notification");
                }

                if !cancelling && *forced.borrow_and_update() {
                    cancelling = true;
                    super::shutdown::contain(|| actor.cancel(), &kill, "cancellation notification");
                }

                tokio::select! {
                    result = completion.observe() => break result,
                    _ = &mut observing, if !joined => joined = true,
                    changed = closed.changed(), if !stopping => {
                        if changed.is_err() {
                            stopping = true;
                            super::shutdown::contain(|| actor.request_shutdown(), &kill, "shutdown notification");
                        }
                    },
                    changed = forced.changed(), if !cancelling => {
                        if changed.is_err() {
                            cancelling = true;
                            super::shutdown::contain(|| actor.cancel(), &kill, "cancellation notification");
                        }
                    },
                }
            };

            drop(alive);

            let mut outcome = AktorOutcome {
                retirement: match &result {
                    Err(error) if !error.physically_joined() => crate::AktorRetireStatus::Unconfirmed,
                    _ if actor.cleanup_completed() => crate::AktorRetireStatus::Confirmed,
                    _ => crate::AktorRetireStatus::Terminated,
                },
                kind,
                actor: name,
                diagnostics: Vec::new(),
                ran_past_grace: actor.cut_call() || actor.cleanup_ran_out(),
            };

            if actor.cleanup_ran_out() {
                outcome
                    .diagnostics
                    .push(AktorError::new("end ran out of its grace", ()));
            }

            if let Err(error) = result {
                let forced = matches!(&*error, OwnerError::Cancelled | OwnerError::CleanupUnconfirmed) && kill.is_stopping();

                let mut first = false;

                if !forced {
                    let (phase, message) = match &*error {
                        OwnerError::Cleanup(errors) => (
                            "cleanup",
                            format!(
                                "actor cleanup failed {} time(s)",
                                errors.errors.len() + errors.diagnostics.len()
                            ),
                        ),
                        OwnerError::Panicked(cause)
                        | OwnerError::PanickedWithCleanup { cause, .. } => {
                            ("completion", format!("actor thread failed: {cause}"))
                        }
                        OwnerError::CleanupUnconfirmed => ("cleanup", error.to_string()),
                        OwnerError::Cancelled | OwnerError::RuntimeStopped => {
                            ("completion", error.to_string())
                        }
                    };

                    first = kill.fail(AktorFailureReport {
                        kind: None,
                        actor: outcome.actor.clone(),
                        phase: phase.into(),
                        message,
                    });
                }

                let (panic, cleanup) = match &*error {
                    OwnerError::Cleanup(cleanup) => (None, Some(cleanup)),
                    OwnerError::PanickedWithCleanup { cause, cleanup } => (Some(cause), Some(cleanup)),
                    OwnerError::Panicked(cause) => (Some(cause), None),
                    OwnerError::CleanupUnconfirmed => (None, None),
                    OwnerError::Cancelled | OwnerError::RuntimeStopped => {
                        outcome.ran_past_grace = true;
                        (None, None)
                    }
                };

                if let Some(cause) = panic && !first && !actor.group_primary() {
                    outcome.diagnostics.push(AktorError::new(format!("panic: {cause}"), ()));
                }

                if let Some(cleanup) = cleanup {
                    outcome.diagnostics.extend(
                        cleanup.errors.iter().filter(|error| !error.primary).map(|error| {
                            AktorError::new(error.diagnostics.clone(), ())
                        }),
                    );
                    outcome.diagnostics.extend(
                        cleanup.diagnostics.iter().cloned(),
                    );
                }
            }

            let _sent = completed.send(outcome);
        });

        if let Err(error) = scheduled {
            let message = error.to_string();
            let primary = self.killswitch().fail_startup(AktorFailureReport {
                kind: Some(kind),
                actor: actor_name.clone(),
                phase: "setup".into(),
                message: message.clone(),
            });
            let _sent = complete.send(AktorOutcome {
                retirement: crate::AktorRetireStatus::Confirmed,
                kind,
                actor: actor_name,
                diagnostics: if primary {
                    Vec::new()
                } else {
                    vec![AktorError::new(message, ())]
                },
                ran_past_grace: false,
            });

            self.completion().wait().await;
            return Err(DedicatedStartError::ThreadNotCreated(error));
        }
        drop(complete);

        let completion = self.completion();
        let result = tokio::select! {
            biased;
            result = ready => result.unwrap_or_else(|_| {
                Err(DedicatedStartError::Panicked {
                    actor: actor_name.clone(),
                    cause: DedicatedJoinError {
                        physically_joined: false,
                        payload: parking_lot::Mutex::new(Box::new("startup observer stopped")),
                    },
                })
            }),
            _ = completion.wait() => Err(DedicatedStartError::Closed),
        };

        if let Err(error) = &result {
            let cancelled = matches!(error.primary(), DedicatedStartError::Closed)
                && !matches!(
                    error,
                    DedicatedStartError::Startup {
                        join: StartupJoin::Failed(_),
                        ..
                    }
                )
                && self.killswitch().is_stopping();

            if !cancelled {
                self.killswitch().fail_startup(AktorFailureReport {
                    kind: None,
                    actor: actor_name,
                    phase: "setup".into(),
                    message: error.to_string(),
                });
            }

            self.completion().wait().await;
        }

        result
    }
}
