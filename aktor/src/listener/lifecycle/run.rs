use super::*;
use crate::FailurePolicy;
use crate::listener::{FailureKind, Failures};
use crate::message::LocalFuture;

#[allow(clippy::too_many_arguments)]
pub fn run<S, E, Initial, C, CleanupFuture>(
    runtime: &crate::executor::Driver,
    state: S,
    mut listener: Listener<S>,
    mut commands: mpsc::Receiver<Command<S, E, C>>,
    status: watch::Sender<bool>,
    started: oneshot::Sender<Result<(), DedicatedStartError<Initial>>>,
    mut cleanup: impl FnMut(S) -> CleanupFuture,
    abandoned_setup: Arc<parking_lot::Mutex<Vec<AbandonedSetup<E>>>>,
    failed_cleanup: Arc<parking_lot::Mutex<FailedCleanup<C>>>,
    force: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CleanupErrors<C>>
where
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    let mut execution = Execution::new(runtime, force);
    let policy = listener.failure.clone();
    let mut failures = Failures::new(listener.name.clone());

    if let FailurePolicy::Group(group) = &policy {
        failures.group = Some(group.clone());
    }

    let mut errors = Vec::new();
    let mut state = Some(state);
    let mut shutdown_reply = None;

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

            let Some(event) = execution.run(async {
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
                        drain(&mut execution, &mut listener, state);
                    }

                    status.send_replace(false);

                    let result = clean(&mut execution, &mut state, &mut cleanup, &mut errors, &failed_cleanup);
                    let _sent = reply.send(result);
                }

                Event::Command(Some(Command::Resume { setup, reply })) => {
                    let result = if state.is_some() {
                        Err(LifecycleError::AlreadyRunning)
                    } else {
                        failed_cleanup.lock().completed = false;
                        match execution.run(async { setup().await }) {
                            Some(Ok(next)) => {
                                state = Some(next);
                                listener.admission.open();
                                status.send_replace(true);
                                Ok(())
                            }
                            Some(Err(error)) => {
                                failed_cleanup.lock().completed = true;
                                Err(LifecycleError::Failed(error))
                            },
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
                        drain(&mut execution, &mut listener, state);
                    }

                    status.send_replace(false);

                    let result = match clean(&mut execution, &mut state, &mut cleanup, &mut errors, &failed_cleanup) {
                        Err(LifecycleError::Failed(error)) => Err(ReplaceError::Cleanup(error)),
                        Err(LifecycleError::Closed | LifecycleError::AlreadyRunning) => Err(ReplaceError::Closed),
                        Ok(()) => {
                            failed_cleanup.lock().completed = false;
                            match execution.run(async { setup().await }) {
                                Some(Ok(next)) => {
                                    state = Some(next);
                                    listener.admission.open();
                                    status.send_replace(true);
                                    Ok(())
                                }
                                Some(Err(error)) => {
                                    failed_cleanup.lock().completed = true;
                                    Err(ReplaceError::Setup(error))
                                }
                                None => Err(ReplaceError::Closed),
                            }
                        }
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
                    shutdown_reply = match event {
                        Event::Command(Some(Command::Shutdown(reply))) => Some(reply),
                        _ => None,
                    };

                    commands.close();
                    listener.close();

                    if let Some(state) = &mut state {
                        execution.run(listener.serve(state));
                    }

                    status.send_replace(false);

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
                        && execution.run(message.run_with(state, &mut listener.hooks)).is_none()
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

    listener.discard_queued(&mut failures);

    // Cleanup can be waiting for resources held by these queued setup closures.
    commands.close();

    while let Ok(command) = commands.try_recv() {
        failures.capture(FailureKind::Teardown, || drop(command));
    }

    failures.capture(FailureKind::Teardown, || drop(commands));

    let cleaned = failures.capture(FailureKind::Cleanup, || {
        clean(
            &mut execution,
            &mut state,
            &mut cleanup,
            &mut errors,
            &failed_cleanup,
        )
    });

    if let Some(reply) = shutdown_reply {
        failures.capture(FailureKind::Teardown, || {
            if let Some(result) = cleaned {
                let _sent = reply.send(result);
            }
        });
    }

    let finished = listener.discard(&mut failures);

    failures.capture(FailureKind::Teardown, || drop(cleanup));

    failures.capture(FailureKind::Teardown, || drop((finished, status)));

    if let Some(failure) = &mut failures.first {
        let mut saved = failed_cleanup.lock();

        saved.errors = std::mem::take(&mut errors);
        saved.diagnostics = failure.diagnostics.clone();
        saved.group_primary = failure.group_primary;
    }

    failures.finish(&policy);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CleanupErrors {
            errors,
            diagnostics: Vec::new(),
        })
    }
}

fn clean<S, C, CleanupFuture>(
    execution: &mut Execution<'_>,
    state: &mut Option<S>,
    cleanup: &mut impl FnMut(S) -> CleanupFuture,
    errors: &mut Vec<Arc<C>>,
    receipt: &Arc<parking_lot::Mutex<FailedCleanup<C>>>,
) -> Result<(), LifecycleError<Arc<C>>>
where
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    let Some(state) = state.take() else {
        return Ok(());
    };
    let result = FailureKind::Cleanup.during(|| {
        execution.runtime.block_on(async {
            let result = cleanup(state).await;
            receipt.lock().completed = true;
            result
        })
    });

    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let error = Arc::new(error);
            errors.push(error.clone());
            Err(LifecycleError::Failed(error))
        }
    }
}

fn drain<S>(execution: &mut Execution<'_>, listener: &mut Listener<S>, state: &mut S) {
    let queued = listener.receiver.len();

    for _ in 0..queued {
        match listener.receiver.try_recv() {
            Ok(message) => {
                if execution
                    .run(message.run_with(state, &mut listener.hooks))
                    .is_none()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

struct Execution<'a> {
    runtime: &'a crate::executor::Driver,
    force: LocalFuture<'static, ()>,
    forced: bool,
}
impl<'a> Execution<'a> {
    fn new(runtime: &'a crate::executor::Driver, mut force: watch::Receiver<bool>) -> Self {
        Self {
            runtime,
            force: Box::pin(async move {
                loop {
                    if *force.borrow_and_update() {
                        return;
                    }

                    if force.changed().await.is_err() {
                        core::future::pending::<()>().await;
                    }
                }
            }),
            forced: false,
        }
    }

    fn run<F: core::future::Future>(&mut self, future: F) -> Option<F::Output> {
        if self.forced {
            return None;
        }

        let result = self.runtime.block_on(async {
            tokio::select! {
                biased;
                _ = &mut self.force => None,
                output = future => Some(output),
            }
        });

        self.forced = result.is_none();
        result
    }
}
