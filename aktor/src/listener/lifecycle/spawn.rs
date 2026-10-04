use super::*;
use crate::FailurePolicy;
use std::thread;
use tokio::{runtime, task};

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
    listener.name = name.clone();

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(DedicatedStartError::Thread)?;

    let group = match &listener.failure {
        FailurePolicy::Group(group) => Some(group.clone()),
        _ => None,
    };
    let (commands, receiver) = mpsc::channel(1);
    let (force, forced) = watch::channel(false);
    let (status, running) = watch::channel(false);
    let abandoned_setup = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let retained_setup = abandoned_setup.clone();
    let failed_cleanup = Arc::new(Mutex::new(FailedCleanup { errors: Vec::new() }));
    let retained_cleanup = failed_cleanup.clone();
    let (started, ready) = oneshot::channel();
    #[cfg(not(target_family = "wasm"))]
    let lifetime = match &listener.failure {
        FailurePolicy::Group(group) => Some(group.track_thread(actor_name.clone())),
        _ => None,
    };

    let thread = thread::Builder::new()
        .name(name)
        .spawn(move || {
            #[cfg(not(target_family = "wasm"))]
            let _lifetime = lifetime;
            let initialized = runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = async {
                        if let Some(group) = &group {
                            group.wait_for_force().await;
                        } else {
                            core::future::pending::<()>().await;
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
                    running,
                    abandoned_setup,
                    failed_cleanup,
                },
                Dedicated { thread, finished },
            ))
        }
        result => {
            let joined = task::spawn_blocking(move || thread.join()).await;

            match result {
                Ok(Err(error)) => Err(error),
                _ => Err(DedicatedStartError::Panicked {
                    actor: actor_name,
                    cause: crate::listener::startup_cause(joined),
                }),
            }
        }
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
