use super::*;
use crate::FailurePolicy;
use std::thread;
#[cfg(feature = "tokio")]
use tokio::task;

/// Start after setup succeeds. Cleanup runs on each pause and final shutdown.
pub async fn spawn_async<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
    args: SpawnArgs<Setup, Cleanup>,
) -> Result<
    (
        Handle<S>,
        Actor<S, E, C>,
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
    spawn_async_with_hooks(args, crate::listener::hooks::AktorHooks::default()).await
}

pub async fn spawn_async_with_hooks<S, E, C, Setup, SetupFuture, Cleanup, CleanupFuture>(
    args: SpawnArgs<Setup, Cleanup>,
    hooks: crate::listener::hooks::AktorHooks<S>,
) -> Result<
    (
        Handle<S>,
        Actor<S, E, C>,
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
    hooks: crate::listener::hooks::AktorHooks<S>,
    standard: bool,
) -> Result<
    (
        Handle<S>,
        Actor<S, E, C>,
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

    let (commands, receiver) = mpsc::channel(1);
    let (force, forced) = watch::channel(false);
    let (shutdown, closing) = watch::channel(false);
    let (status, running) = watch::channel(false);
    let abandoned_setup = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let retained_setup = abandoned_setup.clone();
    let failed_cleanup = Arc::new(Mutex::new(FailedCleanup {
        errors: Vec::new(),
        diagnostics: Vec::new(),
        group_primary: false,
    }));
    let retained_cleanup = failed_cleanup.clone();
    let (mut started, ready) = oneshot::channel();

    #[cfg(not(target_family = "wasm"))]
    let lifetime = match &listener.failure {
        FailurePolicy::Group(group) => Some(group.track_thread(actor_name.clone())),
        _ => None,
    };

    let thread = spawn_owner(name, move || {
        #[cfg(not(target_family = "wasm"))]
        let _lifetime = lifetime;
        let runtime = match crate::executor::Driver::new(standard) {
            Ok(runtime) => runtime,
            Err(error) => {
                drop(listener);
                let _sent = started.send(Err(DedicatedStartError::Thread(error)));
                return Some(Ok(()));
            }
        };

        let initialized = runtime.block_on(async {
            tokio::select! {
                biased;
                _ = async {
                    if let Some(group) = &group {
                        group.wait_for_force_on(standard).await;
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
                receiver,
                status,
                started,
                cleanup,
                retained_setup,
                retained_cleanup,
                forced,
                closing,
            ),
            Some(Err(error)) => {
                drop(listener);
                let _result = started.send(Err(DedicatedStartError::Init(error)));
                Ok(())
            }
            None => {
                drop(listener);
                let _result = started.send(Err(DedicatedStartError::Closed));
                Ok(())
            }
        })
    })
    .map_err(DedicatedStartError::Thread)?;

    match ready.await {
        Ok(Ok(())) => {
            let finished = handle.inner.finished.clone();
            let admission = handle.inner.admission.clone();

            Ok((
                handle,
                Actor {
                    admission,
                    commands,
                    force,
                    shutdown,
                    running,
                    abandoned_setup,
                    failed_cleanup,
                },
                Dedicated { thread, finished },
            ))
        }
        result => {
            #[cfg(feature = "tokio")]
            let cause = {
                let joined = if standard {
                    Ok(thread.join())
                } else {
                    task::spawn_blocking(move || thread.join()).await
                };

                crate::listener::startup_cause(joined)
            };

            #[cfg(not(feature = "tokio"))]
            let cause = standard_cause(thread.join());

            match result {
                Ok(Err(error)) => Err(error),
                _ => Err(DedicatedStartError::Panicked {
                    actor: actor_name,
                    cause,
                }),
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

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use crate::{
        AktorClosures, AktorGroup, AktorKind, AktorName, AktorNew, AktorNewOptions, AktorNoRole,
        AktorSetupError, AktorStartError, aktor_start_in, executor::FAIL_SPAWN,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

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
                name: AktorName::new("already started"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(()),
                    end: Some(
                        (async move |_: ()| {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .into(),
                    ),

                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
        )
        .await
        .unwrap();

        let _reset = Reset;

        FAIL_SPAWN.with(|setting| setting.set(Some("aktor owner")));

        let error = aktor_start_in(
            &group,
            AktorNew {
                name: AktorName::new("cannot create thread"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || -> Result<(), AktorSetupError> {
                        panic!("setup ran after thread creation failed")
                    },
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
        )
        .await
        .err()
        .unwrap();

        let AktorStartError::Thread(DedicatedStartError::Thread(cause)) = error.error else {
            panic!("thread failure lost its typed cause");
        };

        assert_eq!(cause.to_string(), "injected owner thread failure");
        assert!(!error.report.unwrap().timed_out);
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    }
}

pub async fn spawn<S, E, C, Setup, Cleanup>(
    args: SpawnArgs<Setup, Cleanup>,
) -> Result<
    (
        Handle<S>,
        Actor<S, E, C>,
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
        payload: parking_lot::Mutex::new(match joined {
            Err(payload) => payload,
            Ok(_) => Box::new("actor startup ended before ready"),
        }),
    }
}
