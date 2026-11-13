use super::driver::{cancel_callers, capture, contain, poll_owners};
use super::*;

impl<Clock: AktorGroupClock> AktorGroup<Clock> {
    pub async fn finish<Cleanup, CleanupFuture>(self, cleanup: Cleanup) -> AktorShutdownReport
    where
        Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorError>>,
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

        let grace = kill.control.grace;
        let mut order = (0..owners.borrow().len()).rev();
        // The actor, its timer, and whether its calls were cancelled already.
        let mut current: Option<(usize, LocalFuture<'static, ()>, bool)> = None;
        let mut expired = false;

        // Later actors can depend on earlier ones, so they stop first, like drop order. Each
        // gets a grace for its calls and one for `end`. Nothing here can be killed, a stuck
        // actor is left and the next one gets its turn.
        poll_fn(|cx| {
            changed.register(cx);
            poll_owners(&owners, cx, &kill.control);

            loop {
                if let Some((index, until, cancelled)) = &mut current {
                    let index = *index;

                    if !owners.borrow()[index].finished {
                        if until.as_mut().poll(cx).is_pending() {
                            break;
                        }

                        if !*cancelled {
                            *cancelled = true;
                            *until = Clock::wait(Clock::deadline(grace));

                            let entries = core::mem::take(&mut *owners.borrow_mut());

                            if let Some(cancel) = &entries[index].cancel
                                && let Some(error) = contain(cancel)
                            {
                                kill.control.report.borrow_mut().application.push(error);
                            }

                            *owners.borrow_mut() = entries;
                            poll_owners(&owners, cx, &kill.control);
                            continue;
                        }

                        // Release what the expired cleanup holds before the next actor runs its end.
                        let entry = owners.borrow_mut().remove(index);
                        drop_owner(&kill, entry);
                    }

                    current = None;
                }

                let Some(index) = order.next() else {
                    return Poll::Ready(());
                };

                if owners.borrow()[index].finished {
                    continue;
                }

                // The closure can call back into the group, so the owners aren't borrowed meanwhile.
                let entries = core::mem::take(&mut *owners.borrow_mut());

                if let Some(error) = contain(|| (entries[index].shutdown)()) {
                    kill.control.report.borrow_mut().application.push(error);
                }

                *owners.borrow_mut() = entries;
                current = Some((index, Clock::wait(Clock::deadline(grace)), false));
                poll_owners(&owners, cx, &kill.control);
            }

            if timer.as_mut().poll(cx).is_ready() {
                expired = true;
                kill.control.report.borrow_mut().ran_past_grace = true;
                return Poll::Ready(());
            }

            Poll::Pending
        })
        .await;

        // Every actor had its turn, the shutdown closure gets one more grace instead of the rest.
        if !expired {
            timer = Clock::wait(Clock::deadline(grace));
        }

        let entries = core::mem::take(&mut *owners.borrow_mut());

        for entry in entries.into_iter().rev() {
            drop_owner(&kill, entry);
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

fn drop_owner<Clock: AktorGroupClock>(kill: &AktorKillSwitch<Clock>, entry: Entry) {
    let owner_drop = contain(|| drop(entry.owner));
    let shutdown_drop = contain(|| drop(entry.shutdown));

    if !entry.finished {
        let status = (entry.status)();
        let retirement = status.retirement(AktorRetireStatus::Unconfirmed);
        let mut diagnostics = core::mem::take(&mut *entry.diagnostics.borrow_mut());
        diagnostics.extend(status.cleanup_diagnostic);
        let mut report = kill.control.report.borrow_mut();
        report.ran_past_grace = true;
        report.actors.push(AktorOutcome {
            retirement,
            kind: entry.kind,
            actor: entry.name.clone(),
            diagnostics,
            ran_past_grace: true,
        });
    }

    record_drop(kill, &entry.name, entry.kind, owner_drop);
    record_drop(kill, &entry.name, entry.kind, shutdown_drop);
}

async fn finish_hook<Clock, Cleanup, CleanupFuture>(
    kill: &AktorKillSwitch<Clock>,
    cleanup: Cleanup,
    timer: &mut LocalFuture<'static, ()>,
    expired: &mut bool,
) where
    Clock: AktorGroupClock,
    Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
    CleanupFuture: Future<Output = Result<(), AktorError>>,
{
    let mut before_hook = kill.control.report.borrow().clone();

    before_hook.failure = kill.control.failure.borrow().clone();

    let mut hook = Box::pin(async { cleanup(before_hook).await });

    poll_fn(|cx| match capture(|| hook.as_mut().poll(cx)) {
        Ok(Poll::Ready(Ok(()))) => Poll::Ready(()),
        Ok(Poll::Ready(Err(error))) => {
            kill.control.report.borrow_mut().application.push(error);

            Poll::Ready(())
        }
        Err(error) => {
            record_hook(kill, "cleanup", error);
            Poll::Ready(())
        }
        Ok(Poll::Pending) => {
            if *expired || timer.as_mut().poll(cx).is_ready() {
                *expired = true;
                kill.control.report.borrow_mut().callback_timed_out();
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
    kill: &AktorKillSwitch<Clock>,
    name: &str,
    kind: crate::AktorExecution,
    error: Option<AktorError>,
) {
    if let Some(error) = error {
        let primary = kill.fail(AktorFailureReport {
            actor: name.into(),
            kind: Some(kind),
            phase: "owner drop".into(),
            message: error.diagnostics.clone(),
        });

        if !primary {
            kill.control.report.borrow_mut().application.push(error);
        }
    }
}

fn record_hook<Clock: AktorGroupClock>(
    kill: &AktorKillSwitch<Clock>,
    phase: &str,
    error: AktorError,
) {
    let primary = kill.fail(AktorFailureReport {
        kind: None,
        actor: "application".into(),
        phase: phase.into(),
        message: error.diagnostics.clone(),
    });

    if !primary {
        kill.control.report.borrow_mut().application.push(error);
    }
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
                            core::future::pending::<Result<(), String>>().await
                        })
                        .unwrap();
                }

                let mut listener = group
                    .listen_with(async move |_| {
                        cleanup.set(cleanup.get() + 1);
                        core::future::pending::<Result<(), String>>().await
                    })
                    .unwrap();
                group
                    .register_owner(
                        "owner".into(),
                        crate::AktorExecution::Local,
                        Box::new(|| {}),
                        None,
                        None,
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

                assert!(report.ran_past_grace);
                assert_eq!(report.actors[0].ran_past_grace, draining);
                assert_eq!((cleaned.get(), notified.get()), (1, 1));
                let mut published = Box::pin(completed.wait());
                assert!(
                    matches!(published.as_mut().poll(&mut cx), Poll::Ready(report) if report.ran_past_grace)
                );
            }
        }
    }
}
