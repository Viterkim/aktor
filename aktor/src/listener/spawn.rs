use super::*;
use crate::message::{ActorError, ActorResult};
#[cfg(feature = "tokio")]
use core::convert::Infallible;
use tokio::sync::watch;
#[cfg(feature = "tokio")]
use tokio::{
    sync::oneshot,
    task::{self, JoinHandle},
};

pub fn channel<S>(capacity: usize) -> ActorResult<(Handle<S>, Listener<S>)> {
    if capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(ActorError::InvalidCapacity);
    }

    let (sender, receiver) = mailbox::channel(capacity);

    let (finished_sender, finished) = watch::channel(());
    let (alive, handles) = watch::channel(());
    let admission = Arc::new(Admission::new());

    Ok((
        Handle {
            inner: Arc::new(HandleInner {
                sender,
                finished,
                admission: admission.clone(),
                _alive: alive,
            }),
            role: PhantomData,
        },
        Listener {
            name: std::any::type_name::<S>().into(),
            hooks: hooks::AktorHooks::default(),
            receiver,
            failure: FailurePolicy::default(),
            admission,
            handles,
            _finished: finished_sender,
        },
    ))
}

#[cfg(feature = "tokio")]
pub fn startup_cause<T>(
    joined: Result<std::thread::Result<Option<T>>, tokio::task::JoinError>,
) -> DedicatedJoinError {
    let payload: Box<dyn std::any::Any + Send> = match joined {
        Ok(Err(payload)) => payload,
        Ok(Ok(_)) => Box::new("actor startup ended before ready"),
        Err(error) if error.is_panic() => error.into_panic(),
        Err(error) => Box::new(error),
    };

    DedicatedJoinError {
        payload: parking_lot::Mutex::new(payload),
    }
}

#[cfg(feature = "tokio")]
pub fn spawn_local<S: 'static>(
    executor: &task::LocalSet,
    state: S,
    capacity: usize,
) -> ActorResult<(Handle<S>, JoinHandle<S>)> {
    spawn_local_with_policy(executor, state, capacity, FailurePolicy::default())
}

#[cfg(feature = "tokio")]
pub fn spawn_local_with_policy<S: 'static>(
    executor: &task::LocalSet,
    state: S,
    capacity: usize,
    failure: FailurePolicy,
) -> ActorResult<(Handle<S>, JoinHandle<S>)> {
    let (handle, mut listener) = channel(capacity)?;

    listener.failure = failure;

    let task = executor.spawn_local(listener.run(state));

    Ok((handle, task))
}

#[cfg(feature = "tokio")]
pub fn spawn_thread<S: Send + 'static>(
    state: S,
    capacity: usize,
) -> Result<(Handle<S>, Dedicated<S>), DedicatedStartError<Infallible>> {
    spawn_thread_with_policy(state, capacity, FailurePolicy::default())
}

#[cfg(feature = "tokio")]
pub fn spawn_thread_with_policy<S: Send + 'static>(
    state: S,
    capacity: usize,
    failure: FailurePolicy,
) -> Result<(Handle<S>, Dedicated<S>), DedicatedStartError<Infallible>> {
    let (handle, mut listener) =
        channel(capacity).map_err(|_| DedicatedStartError::InvalidCapacity)?;

    listener.failure = failure;

    let thread = thread::Builder::new()
        .name("actor".into())
        .spawn(move || Some(listener.run_blocking(state)))
        .map_err(DedicatedStartError::Thread)?;

    let finished = handle.inner.finished.clone();

    Ok((handle, Dedicated { thread, finished }))
}

/// Set up, serve and clean up on one dedicated thread.
///
/// Cleanup receives the serving outcome, including unwinding panics. After a
/// panic its return value is discarded and the failure policy runs.
#[cfg(feature = "tokio")]
pub async fn spawn_runner<S, E, T, Setup, Serve, Cleanup>(
    args: SpawnArgs<Setup, Cleanup>,
    serve: Serve,
) -> Result<(Handle<S>, Dedicated<T>), DedicatedStartError<E>>
where
    S: 'static,
    E: Send + 'static,
    T: Send + 'static,
    Setup: FnOnce() -> Result<S, E> + Send + 'static,
    Serve: FnOnce(&mut Listener<S>, &mut S) -> Result<(), E> + Send + 'static,
    Cleanup: FnOnce(S, Result<(), RunError<E>>) -> T + Send + 'static,
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

    listener.failure = failure.clone();
    listener.name = name.clone();

    let (started, ready) = oneshot::channel();
    let thread = thread::Builder::new()
        .name(name)
        .spawn(move || match setup() {
            Ok(mut state) => {
                let mut failures = Failures::new(listener.name.clone());
                let outcome = failures.capture(FailureKind::Runtime, || {
                    if started.send(Ok(())).is_ok() {
                        serve(&mut listener, &mut state)
                    } else {
                        Ok(())
                    }
                });

                listener.receiver.close();

                let outcome = match outcome {
                    Some(outcome) => outcome.map_err(RunError::Failed),
                    None => Err(RunError::Panicked),
                };

                let mut result = failures.capture(FailureKind::Cleanup, || cleanup(state, outcome));
                let finished = listener.discard(&mut failures);

                if failures.first.is_some() {
                    failures.capture(FailureKind::Teardown, || drop(result.take()));
                }

                failures.capture(FailureKind::Teardown, || drop(finished));
                failures.finish(&failure);

                result
            }
            Err(error) => {
                drop(listener);
                let _result = started.send(Err(error));
                None
            }
        })
        .map_err(DedicatedStartError::Thread)?;

    match ready.await {
        Ok(Ok(())) => {
            let finished = handle.inner.finished.clone();
            Ok((handle, Dedicated { thread, finished }))
        }
        result => {
            let joined = task::spawn_blocking(move || thread.join()).await;

            match result {
                Ok(Err(error)) => Err(DedicatedStartError::Init(error)),
                _ => Err(DedicatedStartError::Panicked {
                    actor: actor_name,
                    cause: startup_cause(joined),
                }),
            }
        }
    }
}
