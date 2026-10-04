use super::*;
#[cfg(feature = "tokio")]
use crate::{
    Aktor, AktorSetupError, FailurePolicy, SpawnArgs, listener::DedicatedJoinError,
    listener::DedicatedStartError, owner::OwnerError,
};

pub fn watchdog(kill: &KillSwitch) {
    let control = kill.control.clone();
    let deadline = kill.control.changed.borrow().unwrap_or_else(Instant::now);
    let watchdog = std::thread::Builder::new()
        .name("aktor shutdown".into())
        .spawn(move || {
            let mut state = control.lock();
            loop {
                if state.finished && !state.force_exit && state.running.is_empty() {
                    return;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    state.report.timed_out = true;
                    eprintln!("{}", state.report);
                    std::process::exit(1);
                }
                let (next, _) = control
                    .wake
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|error| error.into_inner());
                state = next;
            }
        });
    if let Err(error) = watchdog {
        eprintln!("Could not start the shutdown watchdog: {error}");
        std::process::exit(1);
    }
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

#[cfg(feature = "tokio")]
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
        let ActorArgs {
            name,
            capacity,
            setup,
            cleanup,
        } = args;

        if !self.control.lock().listening {
            return Err(DedicatedStartError::NotStarted);
        }
        let kill = self.killswitch();
        if kill.is_stopping() {
            self.completion().wait().await;
            return Err(DedicatedStartError::Closed);
        }
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                let error = DedicatedStartError::NoRuntime;
                kill.fail(ActorFailure {
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
                entries.push(Entry {
                    name: name.clone(),
                    start: Box::new(move || {
                        closing.send_replace(true);
                    }),
                    cancel: Box::new(move || {
                        forcing.send_replace(true);
                    }),
                    outcome: Box::pin(async move {
                        outcome.await.unwrap_or_else(|_| ActorOutcome {
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

        runtime.spawn(async move {
            let owner = match Aktor::spawn_async(SpawnArgs {
                name: name.clone(),
                capacity,
                failure: FailurePolicy::Group(kill.clone()),
                setup,
                cleanup,
            })
            .await
            {
                Ok(owner) => owner,
                Err(error) => {
                    if matches!(error, DedicatedStartError::Closed) && kill.is_stopping() {
                        kill.control.lock().report.timed_out = true;
                        let _sent = started.send(Err(error));
                        let _sent = completed.send(ActorOutcome {
                            actor: name,
                            diagnostics: Vec::new(),
                            timed_out: true,
                        });
                        return;
                    }
                    let diagnostics = error.to_string();
                    kill.fail(ActorFailure {
                        actor: name.clone(),
                        phase: "setup".into(),
                        message: diagnostics.clone(),
                    });
                    let _sent = started.send(Err(error));
                    let _sent = completed.send(ActorOutcome {
                        actor: name,
                        diagnostics: vec![AktorError::new(diagnostics)],
                        timed_out: false,
                    });
                    return;
                }
            };

            let actor = owner.actor.new_controller();
            let completion = owner.completion();
            let alive = owner.new_handle();
            let _sent = started.send(Ok(owner));
            let mut stopping = false;
            let mut cancelling = false;

            let result = loop {
                if !stopping && *closed.borrow_and_update() {
                    stopping = true;
                    let shutdown = actor.new_controller();
                    tokio::spawn(async move {
                        let _result = shutdown.shutdown().await;
                    });
                }

                if !cancelling && *forced.borrow_and_update() {
                    cancelling = true;
                    actor.cancel();
                }

                tokio::select! {
                    result = completion.wait() => break result,
                    _ = closed.changed(), if !stopping => {},
                    _ = forced.changed(), if !cancelling => {},
                }
            };

            drop(alive);
            let mut outcome = ActorOutcome {
                actor: name,
                diagnostics: Vec::new(),
                timed_out: actor.is_cancelled(),
            };

            if let Err(error) = result {
                let forced = matches!(&*error, OwnerError::Cancelled) && kill.is_stopping();
                if !forced {
                    kill.fail(ActorFailure {
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
