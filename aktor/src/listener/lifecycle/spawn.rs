use super::*;
use crate::listener::FailurePolicy;
use std::thread;
#[cfg(feature = "tokio")]
use tokio::task;

/// Start after setup succeeds. Cleanup runs at shutdown.
#[doc(hidden)]
pub async fn spawn_async<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
    args: SpawnArgs<Setup, Cleanup>,
) -> Result<
    (
        Handle<S>,
        AktorControl<C>,
        Dedicated<Result<(), CleanupErrors<C>>>,
    ),
    DedicatedStartError<E>,
>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Setup: FnOnce() -> SetupFuture + Send + 'static,
    SetupFuture: core::future::Future<Output = Result<S, E>>,
    Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    spawn_async_with_hooks(args, crate::listener::hooks::CallHooks::default()).await
}

#[doc(hidden)]
pub async fn spawn_async_with_hooks<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
    args: SpawnArgs<Setup, Cleanup>,
    hooks: crate::listener::hooks::CallHooks<S>,
) -> Result<
    (
        Handle<S>,
        AktorControl<C>,
        Dedicated<Result<(), CleanupErrors<C>>>,
    ),
    DedicatedStartError<E>,
>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Setup: FnOnce() -> SetupFuture + Send + 'static,
    SetupFuture: core::future::Future<Output = Result<S, E>>,
    Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    spawn_async_on(args, hooks, false).await
}

pub async fn spawn_async_on<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
    args: SpawnArgs<Setup, Cleanup>,
    hooks: crate::listener::hooks::CallHooks<S>,
    standard: bool,
) -> Result<
    (
        Handle<S>,
        AktorControl<C>,
        Dedicated<Result<(), CleanupErrors<C>>>,
    ),
    DedicatedStartError<E>,
>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Setup: FnOnce() -> SetupFuture + Send + 'static,
    SetupFuture: core::future::Future<Output = Result<S, E>>,
    Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    spawn_async_initial_on::<S, E, C, _, _, _, _>(
        args,
        hooks,
        standard,
        None,
        crate::AktorStuck::Kill,
    )
    .await
}

pub async fn spawn_async_initial_on<S, Initial, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
    args: SpawnArgs<Setup, Cleanup>,
    hooks: crate::listener::hooks::CallHooks<S>,
    standard: bool,
    cancelled: Option<watch::Receiver<bool>>,
    stuck: crate::AktorStuck,
) -> Result<
    (
        Handle<S>,
        AktorControl<C>,
        Dedicated<Result<(), CleanupErrors<C>>>,
    ),
    DedicatedStartError<Initial>,
>
where
    S: 'static,
    Initial: Send + 'static,
    C: Send + Sync + 'static,
    Setup: FnOnce() -> SetupFuture + Send + 'static,
    SetupFuture: core::future::Future<Output = Result<S, Initial>>,
    Cleanup: FnMut(S) -> CleanupFuture + Send + 'static,
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    // Threads are tracked natively only, `stuck` decides for them there.
    #[cfg(target_family = "wasm")]
    let _ = stuck;

    let SpawnArgs {
        name,
        capacity,
        failure,
        setup,
        cleanup,
    } = args;

    let actor_name = name.clone();
    let (handle, mut listener) =
        channel(capacity).map_err(|_| DedicatedStartError::InvalidCapacity)?;

    if let FailurePolicy::Group(group) = &failure {
        listener.admission.manage(name.clone(), group.clone());
    }

    listener.failure = failure;
    listener.hooks = hooks;
    listener.name = name.clone();

    listener.admission.set_standard(standard);

    let group = match &listener.failure {
        FailurePolicy::Group(group) => Some(group.clone()),
        _ => None,
    };

    let (force, forced) = watch::channel(false);
    let (shutdown, closing) = watch::channel(false);
    let (status, running) = watch::channel(false);
    let (returned, receipt) = watch::channel(None);
    let failed_cleanup = Arc::new(Mutex::new(FailedCleanup {
        ran_out: false,
        cut_call: false,
        errors: Vec::new(),
        diagnostics: Vec::new(),
        group_primary: false,
    }));
    let retained_cleanup = failed_cleanup.clone();
    let (mut started, ready) = oneshot::channel();

    #[cfg(not(target_family = "wasm"))]
    let lifetime = match &listener.failure {
        FailurePolicy::Group(group) => Some(group.track_thread(actor_name.clone(), stuck)),
        _ => None,
    };

    let thread = spawn_owner(name, move || {
        #[cfg(not(target_family = "wasm"))]
        let _lifetime = lifetime;
        let runtime = match crate::executor::Driver::new(standard) {
            Ok(runtime) => runtime,
            Err(error) => {
                let _sent = started.send(Err(DedicatedStartError::Thread(error)));
                drop(listener);
                return Some(Ok(()));
            }
        };

        let initialized = runtime.block_on(async {
            tokio::select! {
                biased;
                _ = async {
                    if let Some(group) = &group {
                        // The group cancels a stuck start on its turn, before the group deadline.
                        let own = async {
                            match cancelled {
                                Some(mut cancelled) => {
                                    let _ = cancelled.wait_for(|cancelled| *cancelled).await;
                                }
                                None => core::future::pending().await,
                            }
                        };

                        tokio::select! {
                            _ = group.wait_for_force_on(standard) => {},
                            _ = own => {},
                        }
                    } else {
                        started.closed().await;
                    }
                } => None,
                result = async { setup().await } => Some(result),
            }
        });

        Some(match initialized {
            Some(Ok(state)) => run::run(
                &runtime,
                state,
                listener,
                status,
                started,
                cleanup,
                retained_cleanup,
                returned,
                forced,
                closing,
                standard,
            ),
            Some(Err(error)) => {
                let _result = started.send(Err(DedicatedStartError::Init(error)));
                drop(listener);
                Ok(())
            }
            None => {
                let _result = started.send(Err(DedicatedStartError::Closed));
                drop(listener);
                Ok(())
            }
        })
    })
    .map_err(DedicatedStartError::ThreadNotCreated)?;

    match ready.await {
        Ok(Ok(())) => {
            let finished = handle.inner.finished.clone();
            let admission = handle.inner.admission.clone();

            Ok((
                handle,
                AktorControl {
                    admission,
                    force,
                    shutdown,
                    running,
                    failed_cleanup,
                    cleanup: receipt,
                },
                Dedicated { thread, finished },
            ))
        }
        result => {
            #[cfg(feature = "tokio")]
            let joined = {
                if standard {
                    Ok(thread.join())
                } else {
                    task::spawn_blocking(move || thread.join()).await
                }
            };

            #[cfg(not(feature = "tokio"))]
            let joined = thread.join();

            match result {
                Ok(Err(error)) => {
                    #[cfg(feature = "tokio")]
                    let join = crate::listener::startup_join(joined);
                    #[cfg(not(feature = "tokio"))]
                    let join = match joined {
                        Ok(_) => super::super::StartupJoin::Joined,
                        failed => {
                            super::super::StartupJoin::Failed(Arc::new(standard_cause(failed)))
                        }
                    };

                    Err(DedicatedStartError::Startup {
                        cause: Box::new(error),
                        join,
                    })
                }
                _ => {
                    #[cfg(feature = "tokio")]
                    let cause = crate::listener::startup_cause(joined);
                    #[cfg(not(feature = "tokio"))]
                    let cause = standard_cause(joined);

                    Err(DedicatedStartError::Panicked {
                        actor: actor_name,
                        cause,
                    })
                }
            }
        }
    }
}

fn spawn_owner<T: Send + 'static>(
    name: String,
    owner: impl FnOnce() -> T + Send + 'static,
) -> std::io::Result<thread::JoinHandle<T>> {
    #[cfg(test)]
    if crate::executor::FAIL_SPAWN.with(|setting| setting.get()) == Some("aktor owner") {
        return Err(std::io::Error::other("injected owner thread failure"));
    }

    thread::Builder::new().name(name).spawn(owner)
}

#[doc(hidden)]
pub async fn spawn<S, E, C, Setup, Cleanup>(
    args: SpawnArgs<Setup, Cleanup>,
) -> Result<
    (
        Handle<S>,
        AktorControl<C>,
        Dedicated<Result<(), CleanupErrors<C>>>,
    ),
    DedicatedStartError<E>,
>
where
    S: 'static,
    E: Send + 'static,
    C: Send + Sync + 'static,
    Setup: FnOnce() -> Result<S, E> + Send + 'static,
    Cleanup: FnMut(S) -> Result<(), C> + Send + 'static,
{
    let SpawnArgs {
        name,
        capacity,
        failure,
        setup,
        mut cleanup,
    } = args;

    spawn_async(SpawnArgs {
        name,
        capacity,
        failure,
        setup: move || core::future::ready(setup()),
        cleanup: move |state| core::future::ready(cleanup(state)),
    })
    .await
}

#[cfg(not(feature = "tokio"))]
fn standard_cause<T>(joined: std::thread::Result<T>) -> crate::listener::DedicatedJoinError {
    crate::listener::DedicatedJoinError {
        physically_joined: true,
        payload: parking_lot::Mutex::new(match joined {
            Err(payload) => payload,
            Ok(_) => Box::new("actor startup ended before ready"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "tokio")]
    use crate::{
        AktorGroup, AktorKind, AktorNew, AktorStartCause, aktor_start_in, executor::FAIL_SPAWN,
    };
    #[cfg(feature = "tokio")]
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Tail;
    impl Drop for Tail {
        fn drop(&mut self) {
            panic!("startup tail failed");
        }
    }

    #[tokio::test]
    async fn startup_join() {
        for standard in [true, false] {
            if !standard && !cfg!(feature = "tokio") {
                continue;
            }

            let tail = Tail;
            let error = spawn_async_on(
                SpawnArgs {
                    name: "startup join".into(),
                    capacity: 1,
                    failure: FailurePolicy::Unwind,
                    setup: || core::future::ready(Err::<(), _>(17_u8)),
                    cleanup: |_: ()| core::future::ready(Ok::<_, ()>(())),
                },
                crate::listener::hooks::CallHooks {
                    before_each: Some(Box::new(move |_, _| {
                        let _retained = &tail;
                    })),
                    after_each: None,
                },
                standard,
            )
            .await
            .err()
            .unwrap();

            let DedicatedStartError::Startup {
                cause,
                join: crate::listener::StartupJoin::Failed(join),
            } = error
            else {
                panic!("startup failure lost its join result");
            };

            assert!(matches!(*cause, DedicatedStartError::Init(17)));
            assert!(join.physically_joined);
            assert_eq!(join.to_string(), "startup tail failed");
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn supervisor_failure() {
        let startup = crate::aktor_start(crate::AktorSetup {
            actors: AktorNew {
                name: "cannot schedule supervisor".into(),
                kind: AktorKind::TokioThread,
                start: async || crate::aktor_ready(()),
                end: (),
                hooks: Default::default(),
                capacity: 1,
                stuck: crate::AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: core::time::Duration::from_secs(1),
        });
        let completion = startup.completion();

        FAIL_SPAWN.with(|setting| setting.set(Some("aktor supervisor")));
        let (error, _) = startup.await.err().unwrap();
        FAIL_SPAWN.with(|setting| setting.set(None));

        assert_eq!(error.init, crate::AktorInitOutcome::NotRun);
        completion.wait_retired().await.unwrap();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn thread_failure() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                FAIL_SPAWN.with(|setting| setting.set(None));
            }
        }

        let mut group = AktorGroup::new();

        group.start().unwrap();

        let cleaned = Arc::new(AtomicUsize::new(0));
        let count = cleaned.clone();
        let _actor = aktor_start_in(
            &group,
            AktorNew {
                name: "already started".into(),
                kind: AktorKind::TokioThread,
                start: async || Ok::<_, String>(()),
                end: async move |_: ()| {
                    count.fetch_add(1, Ordering::SeqCst);
                },
                hooks: Default::default(),
                capacity: 32,
                stuck: crate::AktorStuck::Kill,
            },
        )
        .await
        .unwrap();

        let _reset = Reset;

        FAIL_SPAWN.with(|setting| setting.set(Some("aktor owner")));

        let (error, _) = aktor_start_in(
            &group,
            AktorNew {
                name: "cannot create thread".into(),
                kind: AktorKind::TokioThread,
                start: async || -> Result<(), String> {
                    panic!("setup ran after thread creation failed")
                },
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: crate::AktorStuck::Kill,
            },
        )
        .await
        .err()
        .unwrap();

        let AktorStartCause::Thread(DedicatedStartError::ThreadNotCreated(cause)) = error.cause
        else {
            panic!("thread failure lost its typed cause");
        };

        assert_eq!(error.init, crate::AktorInitOutcome::NotRun);
        assert_eq!(cause.to_string(), "injected owner thread failure");
        assert!(!error.report.unwrap().ran_past_grace);
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    }
}
