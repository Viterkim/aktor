use super::*;
#[cfg(any(feature = "tokio", feature = "std_thread"))]
use crate::{
    Aktor, AktorSetupError, FailurePolicy, SpawnArgs, listener::DedicatedJoinError,
    listener::DedicatedStartError, owner::OwnerError,
};

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
    ) -> Result<Aktor<S, AktorSetupError, AktorCleanupError>, DedicatedStartError<AktorSetupError>>
    {
        let setup = move || Ok::<_, AktorSetupError>(value);
        let cleanup = |_| Ok::<_, AktorCleanupError>(());
        self.spawn(ActorArgs::new(name, setup, cleanup)).await
    }

    /// Start an actor in this group.
    pub async fn spawn<S, E, C, Setup, Cleanup>(
        &mut self,
        args: ActorArgs<Setup, Cleanup>,
    ) -> Result<
        Aktor<S, AktorSetupError<E>, AktorCleanupError<C>>,
        DedicatedStartError<AktorSetupError<E>>,
    >
    where
        S: 'static,
        E: Send + 'static,
        C: Send + Sync + 'static,
        Setup: FnOnce() -> Result<S, AktorSetupError<E>> + Send + 'static,
        Cleanup: FnMut(S) -> Result<(), AktorCleanupError<C>> + Send + 'static,
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

    pub async fn spawn_async<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
        &mut self,
        args: ActorArgs<Setup, Cleanup>,
    ) -> Result<
        Aktor<S, AktorSetupError<E>, AktorCleanupError<C>>,
        DedicatedStartError<AktorSetupError<E>>,
    >
    where
        S: 'static,
        E: Send + 'static,
        C: Send + Sync + 'static,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, AktorSetupError<E>>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<C>>>,
    {
        self.spawn_async_with_hooks(args, crate::listener::hooks::AktorHooks::default())
            .await
    }

    pub async fn spawn_async_with_hooks<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
        &mut self,
        args: ActorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
    ) -> Result<
        Aktor<S, AktorSetupError<E>, AktorCleanupError<C>>,
        DedicatedStartError<AktorSetupError<E>>,
    >
    where
        S: 'static,
        E: Send + 'static,
        C: Send + Sync + 'static,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, AktorSetupError<E>>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<C>>>,
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
    pub async fn spawn_async_on<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
        &mut self,
        args: ActorArgs<Setup, Cleanup>,
        hooks: crate::listener::hooks::AktorHooks<S>,
        standard: bool,
    ) -> Result<
        Aktor<S, AktorSetupError<E>, AktorCleanupError<C>>,
        DedicatedStartError<AktorSetupError<E>>,
    >
    where
        S: 'static,
        E: Send + 'static,
        C: Send + Sync + 'static,
        Setup: FnOnce() -> SetupFuture + Send + 'static,
        SetupFuture: Future<Output = Result<S, AktorSetupError<E>>>,
        Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<C>>>,
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

                kill.fail(ActorFailure {
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
                            diagnostics: vec![AktorCleanupError::new("startup observer stopped")],
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
            let (owner, observing) = match Aktor::spawn_observed(
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

                        let _sent = started.send(Err(error));
                        let _sent = completed.send(ActorOutcome {
                            kind: None,
                            actor: name,
                            diagnostics: Vec::new(),
                            timed_out: true,
                        });

                        return;
                    }

                    let diagnostics = error.to_string();

                    kill.fail(ActorFailure {
                        kind: None,
                        actor: name.clone(),
                        phase: "setup".into(),
                        message: diagnostics,
                    });

                    let _sent = started.send(Err(error));
                    let _sent = completed.send(ActorOutcome {
                        kind: None,
                        actor: name,
                        diagnostics: Vec::new(),
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
                let forced = matches!(&*error, OwnerError::Cancelled) && kill.is_stopping();

                if !forced {
                    kill.fail(ActorFailure {
                        kind: None,
                        actor: outcome.actor.clone(),
                        phase: if matches!(&*error, OwnerError::Cleanup(_)) {
                            "cleanup"
                        } else {
                            "completion"
                        }
                        .into(),
                        message: error.to_string(),
                    });
                }

                match &*error {
                    OwnerError::Cleanup(errors) => {
                        outcome
                            .diagnostics
                            .extend(errors.errors.iter().map(|error| error.report()));
                    }
                    OwnerError::PanickedWithCleanup { cleanup, .. } => {
                        outcome
                            .diagnostics
                            .extend(cleanup.errors.iter().map(|error| error.report()));
                    }
                    OwnerError::Panicked(_) => {}
                    OwnerError::Cancelled | OwnerError::RuntimeStopped => outcome.timed_out = true,
                }
            }

            let _sent = completed.send(outcome);
        });

        if let Err(error) = scheduled {
            let kill = self.killswitch();

            kill.fail(ActorFailure {
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
                self.killswitch().fail(ActorFailure {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watchdog_policy() {
        for finished in [false, true] {
            for force_exit in [false, true] {
                for running in [false, true] {
                    let mut state = State {
                        finished,
                        force_exit,
                        ..State::default()
                    };

                    if running {
                        state.running.push(Arc::new("owner".into()));
                    }

                    assert_eq!(settled(&state), finished && !force_exit && !running);
                }
            }
        }
    }
}
