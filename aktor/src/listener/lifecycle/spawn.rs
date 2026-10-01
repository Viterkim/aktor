use super::*;
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
    listener.failure = failure;
    listener.name = name.clone();

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(DedicatedStartError::Thread)?;

    let (commands, receiver) = mpsc::channel(1);
    let (status, running) = watch::channel(false);
    let abandoned_setup = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let retained_setup = abandoned_setup.clone();
    let (started, ready) = oneshot::channel();

    let thread = thread::Builder::new()
        .name(name)
        .spawn(move || {
            Some(match runtime.block_on(async { setup().await }) {
                Ok(state) => run::run(
                    &runtime,
                    state,
                    listener,
                    receiver,
                    status,
                    started,
                    cleanup,
                    retained_setup,
                ),
                Err(error) => {
                    drop(listener);
                    let _result = started.send(Err(error));
                    Ok(())
                }
            })
        })
        .map_err(DedicatedStartError::Thread)?;

    match ready.await {
        Ok(Ok(())) => {
            let finished = handle.inner.finished.clone();
            Ok((
                handle,
                Actor {
                    commands,
                    running,
                    abandoned_setup,
                },
                Dedicated { thread, finished },
            ))
        }
        result => {
            let joined = task::spawn_blocking(move || thread.join()).await;

            match result {
                Ok(Err(error)) => Err(DedicatedStartError::Init(error)),
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
