use super::driver::{cancel_callers, capture, contain, poll_owners};
use super::*;
use alloc::string::ToString;

impl<Clock: AktorGroupClock> AktorGroup<Clock> {
    pub(super) async fn finish<Cleanup, CleanupFuture, E>(self, cleanup: Cleanup) -> ShutdownReport
    where
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
    {
        let owners = self.owners.clone();
        let kill = self.killswitch();

        kill.stop();
        cancel_callers(&kill.control);

        let deadline = kill
            .control
            .deadline
            .get()
            .unwrap_or_else(|| Clock::deadline(Duration::ZERO));
        let mut timer = Clock::wait(deadline);
        let changed = kill.control.changed.listen();

        let entries = core::mem::take(&mut *owners.borrow_mut());

        for entry in entries.iter().rev() {
            if let Some(error) = contain(|| (entry.shutdown)()) {
                kill.control.report.borrow_mut().application.push(error);
            }
        }

        *owners.borrow_mut() = entries;

        let mut expired = false;

        poll_fn(|cx| {
            changed.register(cx);
            poll_owners(&owners, cx, &kill.control);

            if owners.borrow().iter().all(|entry| entry.finished) {
                return Poll::Ready(());
            }

            if timer.as_mut().poll(cx).is_ready() {
                expired = true;
                kill.control.report.borrow_mut().timed_out = true;
                return Poll::Ready(());
            }

            Poll::Pending
        })
        .await;

        let entries = core::mem::take(&mut *owners.borrow_mut());

        for entry in entries {
            if !entry.finished {
                kill.control.report.borrow_mut().actors.push(ActorOutcome {
                    kind: Some(entry.kind),
                    actor: entry.name.clone(),
                    diagnostics: Vec::new(),
                    timed_out: true,
                });
            }

            record_drop(
                &kill,
                &entry.name,
                entry.kind,
                contain(|| drop(entry.owner)),
            );

            record_drop(
                &kill,
                &entry.name,
                entry.kind,
                contain(|| drop(entry.shutdown)),
            );
        }

        finish_hook(&kill, cleanup, &mut timer, &mut expired).await;

        let hook = kill.control.shutdown_hook.borrow_mut().take();

        if let Some(hook) = hook {
            finish_hook(&kill, hook, &mut timer, &mut expired).await;
        }

        let mut report = kill.control.report.borrow().clone();

        report.failure = kill.control.failure.borrow().clone();
        *kill.control.completed.borrow_mut() = Some(report.clone());
        kill.control.changed.notify();
        report
    }
}

async fn finish_hook<Clock, Cleanup, CleanupFuture, E>(
    kill: &KillSwitch<Clock>,
    cleanup: Cleanup,
    timer: &mut LocalFuture<'static, ()>,
    expired: &mut bool,
) where
    Clock: AktorGroupClock,
    Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
    CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
{
    let mut before_hook = kill.control.report.borrow().clone();

    before_hook.failure = kill.control.failure.borrow().clone();

    let mut hook = Box::pin(async { cleanup(before_hook).await });

    poll_fn(|cx| match capture(|| hook.as_mut().poll(cx)) {
        Ok(Poll::Ready(Ok(()))) => Poll::Ready(()),
        Ok(Poll::Ready(Err(error))) => {
            kill.control
                .report
                .borrow_mut()
                .application
                .push(error.report());

            if let Some(error) = contain(|| drop(error)) {
                record_hook(kill, "cleanup error drop", error);
            }

            Poll::Ready(())
        }
        Err(error) => {
            record_hook(kill, "cleanup", error);
            Poll::Ready(())
        }
        Ok(Poll::Pending) => {
            if *expired || timer.as_mut().poll(cx).is_ready() {
                *expired = true;
                kill.control.report.borrow_mut().timed_out = true;
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    })
    .await;

    if let Some(error) = contain(|| drop(hook)) {
        record_hook(kill, "cleanup drop", error);
    }
}

fn record_drop<Clock: AktorGroupClock>(
    kill: &KillSwitch<Clock>,
    name: &str,
    kind: crate::AktorExecution,
    error: Option<AktorError>,
) {
    if let Some(error) = error {
        kill.fail(ActorFailure {
            actor: name.into(),
            kind: Some(kind),
            phase: "owner drop".into(),
            message: error.to_string(),
        });

        kill.control.report.borrow_mut().application.push(error);
    }
}

fn record_hook<Clock: AktorGroupClock>(kill: &KillSwitch<Clock>, phase: &str, error: AktorError) {
    kill.fail(ActorFailure {
        kind: None,
        actor: "application".into(),
        phase: phase.into(),
        message: error.to_string(),
    });

    kill.control.report.borrow_mut().application.push(error);
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use core::{
        pin::Pin,
        task::{Context, Waker},
    };

    struct Timer(bool);
    impl Future for Timer {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            assert!(!self.0, "completed shutdown timer polled again");
            self.0 = true;
            Poll::Ready(())
        }
    }

    struct Clock;
    impl AktorGroupClock for Clock {
        type Deadline = ();

        fn deadline(_: Duration) {}

        fn wait(_: ()) -> LocalFuture<'static, ()> {
            Box::pin(Timer(false))
        }
    }

    #[test]
    fn deadline() {
        for draining in [false, true] {
            for synchronous in [false, true] {
                let mut group = AktorGroup::<Clock>::new();
                let completed = group.completion();
                let cleaned = Rc::new(Cell::new(0));
                let cleanup = cleaned.clone();
                let notified = Rc::new(Cell::new(0));
                let notify = notified.clone();

                if synchronous {
                    group
                        .on_shutdown(move |_| notify.set(notify.get() + 1))
                        .unwrap();
                } else {
                    group
                        .on_shutdown(async move |_| {
                            notify.set(notify.get() + 1);
                            core::future::pending::<Result<(), AktorCleanupError>>().await
                        })
                        .unwrap();
                }

                let mut listener = group
                    .listen_with(async move |_| {
                        cleanup.set(cleanup.get() + 1);
                        core::future::pending::<Result<(), AktorCleanupError>>().await
                    })
                    .unwrap();
                group
                    .register_owner(
                        "owner".into(),
                        crate::AktorExecution::Local,
                        Box::new(|| {}),
                        Box::pin(async move {
                            if draining {
                                core::future::pending::<()>().await;
                            }
                            None
                        }),
                    )
                    .unwrap();
                group.killswitch().stop();

                let mut cx = Context::from_waker(Waker::noop());
                let Poll::Ready(report) = listener.as_mut().poll(&mut cx) else {
                    panic!("expired shutdown did not finish");
                };

                assert!(report.timed_out);
                assert_eq!(report.actors[0].timed_out, draining);
                assert_eq!((cleaned.get(), notified.get()), (1, 1));
                let mut published = Box::pin(completed.wait());
                assert!(
                    matches!(published.as_mut().poll(&mut cx), Poll::Ready(report) if report.timed_out)
                );
            }
        }
    }
}
