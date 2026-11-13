use super::*;
use crate::listener::{FailureKind, FailurePolicy, Failures};

#[allow(clippy::too_many_arguments)]
pub fn run<S, Initial, C, CleanupFuture>(
    runtime: &crate::executor::Driver,
    mut state: S,
    mut listener: Listener<S>,
    status: watch::Sender<bool>,
    started: oneshot::Sender<Result<(), DedicatedStartError<Initial>>>,
    mut cleanup: impl FnMut(S) -> CleanupFuture,
    failed_cleanup: Arc<parking_lot::Mutex<FailedCleanup<C>>>,
    returned: watch::Sender<Option<Result<(), Arc<C>>>>,
    mut force: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
    standard: bool,
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
    let mut cut_call = false;

    failures.capture(FailureKind::Runtime, || {
        status.send_replace(true);

        if started.send(Ok(())).is_err() {
            return;
        }

        let stopping = async {
            if shutdown.wait_for(|closing| *closing).await.is_err() {
                core::future::pending::<()>().await;
            }
        };
        let forcing = async {
            if force.wait_for(|forced| *forced).await.is_err() {
                core::future::pending::<()>().await;
            }
        };
        let mut stopping = core::pin::pin!(stopping);
        let mut forcing = core::pin::pin!(forcing);
        let mut draining = false;

        runtime.block_on(async {
            loop {
                let event = tokio::select! {
                    biased;
                    _ = &mut forcing => break,
                    _ = &mut stopping, if !draining => Event::Shutdown,
                    message = listener.receiver.recv() => Event::Message(message),
                };

                match event {
                    Event::Shutdown => {
                        listener.close();
                        draining = true;
                    }
                    Event::Message(Some(message)) => {
                        let running = core::cell::Cell::new(false);
                        let mut call = core::pin::pin!(async {
                            running.set(true);
                            message.run_with(&mut state, &mut listener.hooks).await;
                        });
                        tokio::select! {
                            biased;
                            _ = &mut forcing => {
                                cut_call = running.get();
                                break;
                            },
                            _ = &mut call => (),
                        }
                    }
                    Event::Message(None) => break,
                }
            }
        });
    });

    failed_cleanup.lock().cut_call = cut_call;

    failures.capture(FailureKind::Teardown, || listener.close());
    failures.capture(FailureKind::Teardown, || {
        status.send_replace(false);
    });

    listener.discard_queued(&mut failures);

    failures.capture(FailureKind::Cleanup, || {
        clean(
            runtime,
            state,
            &mut cleanup,
            &mut errors,
            &failed_cleanup,
            &returned,
            (&policy, standard),
        )
    });

    failures.capture(FailureKind::Teardown, || drop(returned));

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
    runtime: &crate::executor::Driver,
    state: S,
    cleanup: &mut impl FnMut(S) -> CleanupFuture,
    errors: &mut Vec<Arc<C>>,
    failed_cleanup: &parking_lot::Mutex<FailedCleanup<C>>,
    returned: &watch::Sender<Option<Result<(), Arc<C>>>>,
    (policy, standard): (&FailurePolicy, bool),
) where
    CleanupFuture: core::future::Future<Output = Result<(), C>>,
{
    let result = FailureKind::Cleanup.during(|| {
        runtime.block_on(async {
            let end = cleanup(state);

            // In a group `end` gets its own grace, so one parked on an await can't hang shutdown.
            match policy {
                FailurePolicy::Group(group) => group.end_within(end, standard).await,
                _ => Some(end.await),
            }
        })
    });

    let Some(result) = result else {
        failed_cleanup.lock().ran_out = true;
        return;
    };

    let result = result.map_err(|error| {
        let error = Arc::new(error);
        errors.push(error.clone());
        error
    });
    returned.send_replace(Some(result));
}
