use super::*;
#[cfg(any(feature = "tokio", feature = "std_thread"))]
use crate::{
    Aktor, AktorDiagnostic, AktorSetupError, FailurePolicy, SpawnArgs,
    listener::{DedicatedJoinError, DedicatedStartError},
    owner::{OwnerFailure, StartError},
};
#[cfg(any(feature = "tokio", feature = "std_thread"))]
use crate::{AktorCleanupOutput, AktorExecution, AktorFailure};

pub fn watchdog(kill: &KillSwitch) {
    let control = kill.control.clone();
    let deadline = kill.control.lock().deadline.unwrap_or_else(Instant::now);
    let watchdog = std::thread::Builder::new()
        .name("aktor shutdown".into())
        .spawn(move || {
            let mut state = control.lock();

            loop {
                if settled(&state) {
                    return;
                }

                let remaining = deadline.saturating_duration_since(Instant::now());

                if remaining.is_zero() {
                    state.report.timed_out = true;
                    drop(state);
                    control.wake.notify_all();
                    terminate();
                }

                let (next, _) = control
                    .wake
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|error| error.into_inner());

                state = next;
            }
        });

    if watchdog.is_err() {
        terminate();
    }

    let control = kill.control.clone();
    let reserve = (kill.control.grace / 10).min(Duration::from_millis(100));
    let reporting_at = deadline.checked_sub(reserve / 8).unwrap_or(deadline);
    let _ = std::thread::Builder::new()
        .name("aktor shutdown report".into())
        .spawn(move || {
            let report = {
                let mut state = control.lock();

                loop {
                    if settled(&state) {
                        return;
                    }

                    let remaining = reporting_at.saturating_duration_since(Instant::now());

                    if remaining.is_zero() {
                        break state.report.clone();
                    }

                    let (next, _) = control
                        .wake
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|error| error.into_inner());

                    state = next;
                }
            };

            eprintln!("Shutdown still pending:\n{report}");
        });
}

fn settled(state: &State) -> bool {
    state.finished && !state.force_exit && state.running.is_empty()
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

impl KillSwitch {
    #[doc(hidden)]
    pub fn track_thread(&self, name: String) -> ThreadLife {
        let name = Arc::new(name);

        self.control.lock().running.push(name.clone());
        ThreadLife {
            control: self.control.clone(),
            name,
        }
    }
}

impl Drop for ThreadLife {
    fn drop(&mut self) {
        self.control
            .lock()
            .running
            .retain(|name| !Arc::ptr_eq(name, &self.name));
        self.control.wake.notify_all();
        self.control.threads.notify_one();
    }
}

#[cfg(any(feature = "tokio", feature = "std_thread"))]
impl AktorGroup {
    /// Move an existing value into an actor. Cleanup just drops it.
    pub async fn spawn_value<S: Send + 'static>(
        &mut self,
        name: impl Into<String>,
        value: S,
    ) -> Result<Aktor<S>, StartError> {
        let setup = move || Ok::<_, AktorSetupError>(value);
        let cleanup = |_| Ok::<_, AktorCleanupError>(());
        self.spawn(ActorArgs::new(name, setup, cleanup)).await
    }

    /// Start an actor in this group.
    pub async fn spawn<S, Setup, Cleanup, Output>(
        &mut self,
        args: ActorArgs<Setup, Cleanup>,
    ) -> Result<Aktor<S>, StartError>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> Result<S, crate::AktorSetupError> + Send + 'static,
        Cleanup: FnMut(S) -> Output + Send + 'static,
    {
        let ActorArgs {
            name,
            capacity,
            setup,
            mut cleanup,
        } = args;

        self.spawn_async(ActorArgs {
            name,
            capacity,
            setup: move || core::future::ready(setup()),
            cleanup: move |state| core::future::ready(cleanup(state)),
        })
        .await
    }

    pub async fn spawn_async<S, Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        &mut self,
        args: ActorArgs<Setup, Cleanup>,
    ) -> Result<Aktor<S>, StartError>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        self.spawn_async_with_hooks(args, crate::listener::hooks::AktorHooks::default())
            .await
    }

    pub async fn spawn_async_with_hooks<S, Setup, SetupFuture, Cleanup, CleanupFuture, Output>(
        &mut self,
        args: ActorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
    ) -> Result<Aktor<S>, StartError>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError>>,
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
        args: ActorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
        standard: bool,
    ) -> Result<Aktor<S>, StartError>
    where
        S: 'static,
        Output: AktorCleanupOutput,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Output>,
    {
        let ActorArgs {
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
            ActorArgs {
                name,
                capacity,
                setup: move || async move {
                    match setup().await {
                        Ok(state) => Ok(state),
                        Err(error) => Err(kill.startup_error(actor, kind, error)),
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
            |error| error.diagnostics.clone(),
            |error| (error.diagnostics.clone(), error.primary),
        )
        .await
        .map_err(|error| {
            let mut error = StartError::from(error);

            if let Some(report) = self.completion().try_report()
                && report.failed()
            {
                error.diagnostics = Some(AktorDiagnostic::new(
                    "actor startup failed",
                    report.to_string(),
                ));
            }

            error
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
        args: ActorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
        standard: bool,
        describe_cleanup: fn(&AktorFailure) -> String,
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

        let ActorArgs {
            name,
            capacity,
            setup,
            cleanup,
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

                kill.fail_startup(ActorFailure {
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
        let (completed, outcome) = tokio::sync::oneshot::channel();
        let (closing, mut closed) = watch::channel(false);
        let (forcing, mut forced) = watch::channel(false);
        let label = name.clone();

        // Keep startup owned even if the application stops awaiting it.
        let registered = {
            let mut entries = self
                .actors
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            if kill.is_stopping() {
                false
            } else {
                kill.control.lock().kinds.push((name.clone(), kind));
                entries.push(Entry {
                    kind,
                    name: name.clone(),
                    start: Box::new(move || {
                        closing.send_replace(true);
                    }),
                    cancel: Box::new(move || {
                        forcing.send_replace(true);
                    }),
                    outcome: Box::pin(async move {
                        outcome.await.unwrap_or_else(|_| ActorOutcome {
                            kind: None,
                            actor: label,
                            diagnostics: vec![AktorCleanupError::new(
                                "startup observer stopped",
                                (),
                            )],
                            timed_out: false,
                        })
                    }),
                });

                true
            }
        };

        if !registered {
            self.completion().wait().await;
            return Err(DedicatedStartError::Closed);
        }

        let actor_name = name.clone();

        let scheduled = runtime.spawn_named("aktor supervisor", async move {
            let (owner, observing) = match Aktor::<S>::spawn_observed(
                SpawnArgs {
                    name: name.clone(),
                    capacity,
                    failure: FailurePolicy::Group(kill.clone()),
                    setup,
                    cleanup,
                },
                hooks,
                standard,
            )
            .await
            {
                Ok(owner) => owner,
                Err(error) => {
                    if matches!(error, DedicatedStartError::Closed) && kill.is_stopping() {
                        kill.control.lock().report.timed_out = true;

                        let sent = started.send(Err(error));
                        super::shutdown::contain_drop(sent, &kill, "startup reply drop");
                        let _sent = completed.send(ActorOutcome {
                            kind: None,
                            actor: name,
                            diagnostics: Vec::new(),
                            timed_out: true,
                        });

                        return;
                    }

                    let first = kill.fail_startup(ActorFailure {
                        kind: None,
                        actor: name.clone(),
                        phase: "setup".into(),
                        message: "actor initialization failed".into(),
                    });

                    let (diagnostics, primary) = match &error {
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
                        DedicatedStartError::Thread(error) => (error.to_string(), false),
                        DedicatedStartError::Panicked { actor, cause } => {
                            (format!("actor {actor} startup panicked: {cause}"), false)
                        }
                    };

                    if first && let Some(failure) = &mut kill.control.lock().report.failure {
                        failure.message = diagnostics.clone();
                    }

                    let sent = started.send(Err(error));
                    super::shutdown::contain_drop(sent, &kill, "startup reply drop");
                    let _sent = completed.send(ActorOutcome {
                        kind: None,
                        actor: name,
                        diagnostics: if first || primary {
                            Vec::new()
                        } else {
                            vec![AktorError::new(diagnostics, ())]
                        },
                        timed_out: false,
                    });

                    return;
                }
            };

            let mut observing = core::pin::pin!(observing);
            let mut joined = false;
            let actor = owner.actor.new_controller();
            let completion = owner.completion();
            let alive = owner.new_handle();
            let _sent = started.send(Ok(owner));
            let mut stopping = false;
            let mut cancelling = false;

            let result = loop {
                if !stopping && *closed.borrow_and_update() {
                    stopping = true;
                    actor.request_shutdown();
                }

                if !cancelling && *forced.borrow_and_update() {
                    cancelling = true;
                    actor.cancel();
                }

                tokio::select! {
                    result = completion.wait() => break result,
                    _ = &mut observing, if !joined => joined = true,
                    changed = closed.changed(), if !stopping => {
                        if changed.is_err() {
                            stopping = true;
                            actor.request_shutdown();
                        }
                    },
                    changed = forced.changed(), if !cancelling => {
                        if changed.is_err() {
                            cancelling = true;
                            actor.cancel();
                        }
                    },
                }
            };

            drop(alive);

            let mut outcome = ActorOutcome {
                kind: None,
                actor: name,
                diagnostics: Vec::new(),
                timed_out: actor.is_cancelled(),
            };

            if let Err(error) = result {
                let forced = matches!(&error.error, OwnerFailure::Cancelled) && kill.is_stopping();

                let mut first = false;

                if !forced {
                    let (phase, message) = match &error.error {
                        OwnerFailure::Cleanup(errors) => (
                            "cleanup",
                            format!(
                                "actor cleanup failed {} time(s)",
                                errors.errors.len() + errors.diagnostics.len()
                            ),
                        ),
                        OwnerFailure::Panicked(cause)
                        | OwnerFailure::PanickedWithCleanup { cause, .. } => {
                            ("completion", format!("actor thread failed: {cause}"))
                        }
                        OwnerFailure::Cancelled | OwnerFailure::RuntimeStopped => {
                            ("completion", error.to_string())
                        }
                    };

                    first = kill.fail(ActorFailure {
                        kind: None,
                        actor: outcome.actor.clone(),
                        phase: phase.into(),
                        message,
                    });
                }

                match &error.error {
                    OwnerFailure::Cleanup(errors) => {
                        outcome.diagnostics.extend(
                            errors
                                .errors
                                .iter()
                                .map(|error| AktorError::new(describe_cleanup(error), ())),
                        );
                        outcome
                            .diagnostics
                            .extend(errors.diagnostics.iter().cloned());
                    }
                    OwnerFailure::PanickedWithCleanup { cause, cleanup } => {
                        if !first && !actor.group_primary() {
                            outcome
                                .diagnostics
                                .push(AktorError::new(format!("panic: {cause}"), ()));
                        }
                        outcome.diagnostics.extend(
                            cleanup
                                .errors
                                .iter()
                                .map(|error| AktorError::new(describe_cleanup(error), ())),
                        );
                        outcome
                            .diagnostics
                            .extend(cleanup.diagnostics.iter().cloned());
                    }
                    OwnerFailure::Panicked(cause) => {
                        if !first && !actor.group_primary() {
                            outcome
                                .diagnostics
                                .push(AktorError::new(format!("panic: {cause}"), ()));
                        }
                    }
                    OwnerFailure::Cancelled | OwnerFailure::RuntimeStopped => {
                        outcome.timed_out = true
                    }
                }
            }

            let _sent = completed.send(outcome);
        });

        if let Err(error) = scheduled {
            let kill = self.killswitch();

            kill.fail_startup(ActorFailure {
                kind: Some(kind),
                actor: actor_name.clone(),
                phase: "setup".into(),
                message: error.to_string(),
            });

            self.completion().wait().await;
            return Err(DedicatedStartError::Thread(error));
        }

        let result = ready.await.unwrap_or_else(|_| {
            Err(DedicatedStartError::Panicked {
                actor: actor_name.clone(),
                cause: DedicatedJoinError {
                    payload: parking_lot::Mutex::new(Box::new("startup observer stopped")),
                },
            })
        });

        if let Err(error) = &result {
            let cancelled =
                matches!(error, DedicatedStartError::Closed) && self.killswitch().is_stopping();

            if !cancelled {
                self.killswitch().fail_startup(ActorFailure {
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
