use super::*;
use crate::FailurePolicy;
use crate::listener::{FailureKind, Failures};

#[allow(clippy::too_many_arguments)]
pub fn run<S, E, C, CleanupFuture>(
    runtime: &crate::executor::Driver,
    state: S,
    mut listener: Listener<S>,
    mut commands: mpsc::Receiver<Command<S, E, C>>,
    status: watch::Sender<bool>,
    started: oneshot::Sender<Result<(), DedicatedStartError<E>>>,
    mut cleanup: impl FnMut(S) -> CleanupFuture,
    abandoned_setup: Arc<parking_lot::Mutex<Vec<AbandonedSetup<E>>>>,
    failed_cleanup: Arc<parking_lot::Mutex<FailedCleanup<C>>>,
    force: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CleanupErrors<C>>
where
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    let policy = listener.failure.clone();
    let mut failures = Failures::new(listener.name.clone());

    if let FailurePolicy::Group(group) = &policy {
        failures.group = Some(group.clone());
    }

    let mut errors = Vec::new();
    let mut state = Some(state);

    failures.capture(FailureKind::Runtime, || {
        status.send_replace(true);

        if started.send(Ok(())).is_err() {
            return;
        }

        let mut controls_open = true;

        loop {
            if *shutdown.borrow() {
                // Finish committed lifecycle changes before stopping.
                commands.close();
            }

            let Some(event) = execute(runtime, &force, async {
                tokio::select! {
                    biased;
                    command = commands.recv(), if controls_open => Event::Command(command),
                    _ = async {
                        loop {
                            if *shutdown.borrow_and_update() { return; }
                            if shutdown.changed().await.is_err() { core::future::pending::<()>().await; }
                        }
                    } => {
                        commands.close();
                        match commands.try_recv() {
                            Ok(command) => Event::Command(Some(command)),
                            Err(_) => Event::Shutdown,
                        }
                    },
                    message = listener.receiver.recv(), if state.is_some() => Event::Message(message),
                    _ = listener.handles.changed(), if state.is_none() => Event::NoHandles,
                }
            }) else { break; };

            match event {
                Event::Command(Some(Command::Pause(reply))) => {
                    listener.admission.close();

                    if let Some(state) = &mut state {
                        drain(runtime, &force, &mut listener, state);
                    }

                    status.send_replace(false);

                    let result = clean(runtime, &force, &mut state, &mut cleanup, &mut errors).map_err(LifecycleError::Failed);
                    let _sent = reply.send(result);
                }

                Event::Command(Some(Command::Resume { setup, reply })) => {
                    let result = if state.is_some() {
                        Err(LifecycleError::AlreadyRunning)
                    } else {
                        match execute(runtime, &force, async { setup().await }) {
                            Some(Ok(next)) => {
                                state = Some(next);
                                listener.admission.open();
                                status.send_replace(true);
                                Ok(())
                            }
                            Some(Err(error)) => Err(LifecycleError::Failed(error)),
                            None => Err(LifecycleError::Closed),
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
                        drain(runtime, &force, &mut listener, state);
                    }

                    status.send_replace(false);

                    let result = match clean(runtime, &force, &mut state, &mut cleanup, &mut errors) {
                        Err(error) => Err(ReplaceError::Cleanup(error)),
                        Ok(()) => match execute(runtime, &force, async { setup().await }) {
                            Some(Ok(next)) => {
                                state = Some(next);
                                listener.admission.open();
                                status.send_replace(true);
                                Ok(())
                            }
                            Some(Err(error)) => Err(ReplaceError::Setup(error)),
                            None => Err(ReplaceError::Closed),
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

                Event::Shutdown | Event::Command(Some(Command::Shutdown(_))) => {
                    let reply = match event {
                        Event::Command(Some(Command::Shutdown(reply))) => Some(reply),
                        _ => None,
                    };

                    listener.close();

                    if let Some(state) = &mut state {
                        execute(runtime, &force, listener.serve(state));
                    }

                    status.send_replace(false);

                    let result = clean(runtime, &force, &mut state, &mut cleanup, &mut errors).map_err(LifecycleError::Failed);

                    if let Some(reply) = reply {
                        let _sent = reply.send(result);
                    }

                    break;
                }

                Event::Command(None) => {
                    controls_open = false;

                    if state.is_none() {
                        break;
                    }
                }

                Event::Message(Some(message)) => {
                    if let Some(state) = &mut state
                        && execute(runtime, &force, message.run_with(state, &mut listener.hooks)).is_none()
                    {
                        break;
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
        let _result = clean(runtime, &force, &mut state, &mut cleanup, &mut errors);
    });

    // Queued setup closures can panic on drop too.
    commands.close();

    while let Ok(command) = commands.try_recv() {
        failures.capture(FailureKind::Teardown, || drop(command));
    }

    failures.capture(FailureKind::Teardown, || drop(commands));

    let finished = listener.discard(&mut failures);

    failures.capture(FailureKind::Teardown, || drop(cleanup));

    failures.capture(FailureKind::Teardown, || drop((finished, status)));

    if failures.first.is_some() {
        failed_cleanup.lock().errors = std::mem::take(&mut errors);
    }

    failures.finish(&policy);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CleanupErrors { errors })
    }
}

pub fn clean<S, C, CleanupFuture>(
    runtime: &crate::executor::Driver,
    force: &watch::Receiver<bool>,
    state: &mut Option<S>,
    cleanup: &mut impl FnMut(S) -> CleanupFuture,
    errors: &mut Vec<Arc<C>>,
) -> Result<(), Arc<C>>
where
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    if let Some(state) = state.take()
        && let Some(Err(error)) =
            FailureKind::Cleanup.during(|| execute(runtime, force, async { cleanup(state).await }))
    {
        let error = Arc::new(error);
        errors.push(error.clone());
        return Err(error);
    }

    Ok(())
}

pub fn drain<S>(
    runtime: &crate::executor::Driver,
    force: &watch::Receiver<bool>,
    listener: &mut Listener<S>,
    state: &mut S,
) {
    let queued = listener.receiver.len();

    for _ in 0..queued {
        match listener.receiver.try_recv() {
            Ok(message) => {
                if execute(runtime, force, message.run_with(state, &mut listener.hooks)).is_none() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

fn execute<F: core::future::Future>(
    runtime: &crate::executor::Driver,
    force: &watch::Receiver<bool>,
    future: F,
) -> Option<F::Output> {
    let mut force = force.clone();

    runtime.block_on(async move {
        tokio::select! {
            biased;
            _ = async {
                loop {
                    if *force.borrow_and_update() { return; }
                    if force.changed().await.is_err() { core::future::pending::<()>().await; }
                }
            } => None,
            output = future => Some(output),
        }
    })
}
