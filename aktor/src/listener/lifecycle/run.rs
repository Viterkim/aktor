use super::*;
use crate::listener::{FailureKind, Failures};

#[allow(clippy::too_many_arguments)]
pub fn run<S, E, C, CleanupFuture>(
    runtime: &tokio::runtime::Runtime,
    state: S,
    mut listener: Listener<S>,
    mut commands: mpsc::Receiver<Command<S, E, C>>,
    status: watch::Sender<bool>,
    started: oneshot::Sender<Result<(), E>>,
    mut cleanup: impl FnMut(S) -> CleanupFuture,
    abandoned_setup: Arc<parking_lot::Mutex<Vec<AbandonedSetup<E>>>>,
) -> Result<(), CleanupErrors<C>>
where
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    let policy = listener.failure.clone();
    let mut failures = Failures::new(listener.name.clone());
    let mut errors = Vec::new();
    let mut state = Some(state);

    failures.capture(FailureKind::Runtime, || {
        status.send_replace(true);

        if started.send(Ok(())).is_err() {
            return;
        }

        let mut controls_open = true;
        loop {
            let event = runtime.block_on(async {
                tokio::select! {
                    biased;
                    command = commands.recv(), if controls_open => Event::Command(command),
                    message = listener.receiver.recv(), if state.is_some() => Event::Message(message),
                    _ = listener.handles.changed(), if state.is_none() => Event::NoHandles,
                }
            });

            match event {
                Event::Command(Some(Command::Pause(reply))) => {
                    listener.admission.close();
                    if let Some(state) = &mut state {
                        drain(runtime, &mut listener, state);
                    }

                    status.send_replace(false);
                    let result = clean(runtime, &mut state, &mut cleanup, &mut errors).map_err(LifecycleError::Failed);
                    let _sent = reply.send(result);
                }

                Event::Command(Some(Command::Resume { setup, reply })) => {
                    let result = if state.is_some() {
                        Err(LifecycleError::AlreadyRunning)
                    } else {
                        match runtime.block_on(async { setup().await }) {
                            Ok(next) => {
                                state = Some(next);
                                listener.admission.open();
                                status.send_replace(true);
                                Ok(())
                            }
                            Err(error) => Err(LifecycleError::Failed(error)),
                        }
                    };

                    let answer = SetupAnswer::new(result, abandoned_setup.clone(), |result| {
                        match result {
                            Err(LifecycleError::Failed(error)) => Some(AbandonedSetup::Resume(error)),
                            _ => None,
                        }
                    });
                    let _sent = reply.send(answer);
                }

                Event::Command(Some(Command::Replace { setup, reply })) => {
                    listener.admission.close();
                    if let Some(state) = &mut state {
                        drain(runtime, &mut listener, state);
                    }

                    status.send_replace(false);
                    let result = match clean(runtime, &mut state, &mut cleanup, &mut errors) {
                        Err(error) => Err(ReplaceError::Cleanup(error)),
                        Ok(()) => match runtime.block_on(async { setup().await }) {
                            Ok(next) => {
                                state = Some(next);
                                listener.admission.open();
                                status.send_replace(true);
                                Ok(())
                            }
                            Err(error) => Err(ReplaceError::Setup(error)),
                        },
                    };

                    let answer = SetupAnswer::new(result, abandoned_setup.clone(), |result| {
                        match result {
                            Err(ReplaceError::Setup(error)) => Some(AbandonedSetup::Replace(error)),
                            _ => None,
                        }
                    });
                    let _sent = reply.send(answer);
                }

                Event::Command(Some(Command::Shutdown(reply))) => {
                    listener.close();
                    if let Some(state) = &mut state {
                        runtime.block_on(listener.serve(state));
                    }

                    status.send_replace(false);
                    let result = clean(runtime, &mut state, &mut cleanup, &mut errors).map_err(LifecycleError::Failed);
                    let _sent = reply.send(result);

                    break;
                }

                Event::Command(None) => {
                    controls_open = false;

                    if state.is_none() {
                        break;
                    }
                }

                Event::Message(Some(message)) => {
                    if let Some(state) = &mut state {
                        runtime.block_on(message.run(state));
                    }
                }
                Event::Message(None) | Event::NoHandles => break,
            }
        }
    });

    failures.capture(FailureKind::Teardown, || listener.close());
    failures.capture(FailureKind::Teardown, || {
        status.send_replace(false);
    });

    failures.capture(FailureKind::Cleanup, || {
        let _result = clean(runtime, &mut state, &mut cleanup, &mut errors);
    });

    // Queued setup closures can panic on drop too.
    commands.close();
    while let Ok(command) = commands.try_recv() {
        failures.capture(FailureKind::Teardown, || drop(command));
    }

    failures.capture(FailureKind::Teardown, || drop(commands));
    let finished = listener.discard(&mut failures);
    failures.capture(FailureKind::Teardown, || drop(cleanup));

    if failures.first.is_some() {
        for error in errors.drain(..) {
            failures.capture(FailureKind::Teardown, || drop(error));
        }
    }

    failures.capture(FailureKind::Teardown, || drop((finished, status)));
    failures.finish(&policy);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CleanupErrors { errors })
    }
}

pub fn clean<S, C, CleanupFuture>(
    runtime: &tokio::runtime::Runtime,
    state: &mut Option<S>,
    cleanup: &mut impl FnMut(S) -> CleanupFuture,
    errors: &mut Vec<Arc<C>>,
) -> Result<(), Arc<C>>
where
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    if let Some(state) = state.take()
        && let Err(error) =
            FailureKind::Cleanup.during(|| runtime.block_on(async { cleanup(state).await }))
    {
        let error = Arc::new(error);
        errors.push(error.clone());
        return Err(error);
    }

    Ok(())
}

pub fn drain<S>(runtime: &tokio::runtime::Runtime, listener: &mut Listener<S>, state: &mut S) {
    let queued = listener.receiver.len();
    for _ in 0..queued {
        match listener.receiver.try_recv() {
            Ok(message) => runtime.block_on(message.run(state)),
            Err(_) => break,
        }
    }
}
