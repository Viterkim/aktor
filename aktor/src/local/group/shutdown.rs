use super::driver::{
    cancel_callers, capture, contain, drop_task, poll_owners, poll_task, task_failure,
};
use super::*;

impl<Clock: AktorGroupClock> AktorGroup<Clock> {
    pub async fn finish<Cleanup, CleanupFuture>(self, cleanup: Cleanup) -> AktorShutdownReport
    where
        Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
    {
        let owners = self.owners.clone();
        let kill = self.killswitch();

        kill.stop();

        let deadline = kill
            .control
            .deadline
            .get()
            .unwrap_or_else(|| Clock::deadline(Duration::ZERO));
        let mut timer = Clock::wait(deadline);
        let changed = kill.control.changed.listen();

        let grace = kill.control.grace;
        let mut position = owners.borrow().len();
        let mut task: Option<(Application, LocalFuture<'static, ()>)> = None;
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

                            let entries = core::mem::take(&mut *owners.borrow_mut());

                            let entry = &entries[index];
                            if let Some(owner) = kill
                                .control
                                .report
                                .borrow_mut()
                                .actor_mut(entry.id)
                                .and_then(|actor| actor.owner.as_mut())
                            {
                                owner.ran_past_grace = true;
                            }
                            if let Some(cancel) = &entry.cancel {
                                contain(cancel, |error| {
                                    record_drop(&kill, entry.id, &entry.name, entry.kind, error)
                                });
                            }

                            *owners.borrow_mut() = entries;
                            poll_owners(&owners, cx, &kill.control);
                            *until = Clock::wait(Clock::deadline(grace));
                            continue;
                        }

                        poll_owners(&owners, cx, &kill.control);
                        if owners.borrow()[index].finished {
                            continue;
                        }

                        // Release what the expired cleanup holds before the next actor runs its end.
                        let entry = owners.borrow_mut().remove(index);
                        drop_owner(&kill, entry);
                    }

                    current = None;
                }

                if task.is_none() {
                    let mut callers = kill.control.callers.borrow_mut();
                    task = callers
                        .pop_if(|caller| caller.after == position)
                        .map(|caller| (caller, Clock::wait(Clock::deadline(grace))));
                }

                if let Some((caller, until)) = &mut task {
                    if caller.policy == crate::AktorJob::FinishBeforeEnd
                        && poll_task(&kill.control, caller, cx).is_pending()
                    {
                        if until.as_mut().poll(cx).is_pending() {
                            return Poll::Pending;
                        }
                        kill.control
                            .report
                            .borrow_mut()
                            .late(crate::AktorLateWork::Application);
                        task_failure(
                            &kill.control,
                            er_new!(crate::report::ApplicationCut).er_report(),
                        );
                    }
                    if let Some((task, _)) = task.take() {
                        drop_task(&kill.control, task);
                    }
                    continue;
                }

                let Some(index) = position.checked_sub(1) else {
                    return Poll::Ready(());
                };
                position = index;
                if owners.borrow()[index].finished {
                    continue;
                }

                // The closure can call back into the group, so the owners aren't borrowed meanwhile.
                let entries = core::mem::take(&mut *owners.borrow_mut());

                let entry = &entries[index];
                contain(
                    || (entry.shutdown)(),
                    |error| record_drop(&kill, entry.id, &entry.name, entry.kind, error),
                );

                *owners.borrow_mut() = entries;
                current = Some((index, Clock::wait(Clock::deadline(grace)), false));
                poll_owners(&owners, cx, &kill.control);
            }

            if timer.as_mut().poll(cx).is_ready() {
                expired = true;
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

        cancel_callers(&kill.control);
        finish_hook(&kill, cleanup, &mut timer, &mut expired).await;

        let hook = kill.control.shutdown_hook.borrow_mut().take();

        if let Some(hook) = hook {
            finish_hook(&kill, hook, &mut timer, &mut expired).await;
        }

        let report = kill.control.publish().clone();
        kill.control.changed.notify();
        report
    }
}

fn drop_owner<Clock: AktorGroupClock>(kill: &AktorKillSwitch<Clock>, entry: Entry) {
    let record = |error| record_drop(kill, entry.id, &entry.name, entry.kind, error);
    contain(|| drop(entry.owner), record);
    contain(|| drop(entry.shutdown), record);

    if !entry.finished {
        let status = (entry.status)();
        let retirement = status.retirement(AktorRetirement::StillRunning);
        let mut report = kill.control.report.borrow_mut();

        report
            .actor_mut(entry.id)
            .expect("registered actor")
            .owner_outcome(OwnerOutcome {
                retirement,
                init: status.init,
                ran_past_grace: true,
            });
    }
}

async fn finish_hook<Clock, Cleanup, CleanupFuture>(
    kill: &AktorKillSwitch<Clock>,
    cleanup: Cleanup,
    timer: &mut LocalFuture<'static, ()>,
    expired: &mut bool,
) where
    Clock: AktorGroupClock,
    Cleanup: FnOnce(AktorShutdownReport) -> CleanupFuture,
    CleanupFuture: Future<Output = Result<(), (AktorCleanupError, String)>>,
{
    let before_hook = kill.control.report.borrow().snapshot();

    let mut hook = Box::pin(async { cleanup(before_hook).await });

    poll_fn(|cx| {
        match capture(
            || hook.as_mut().poll(cx),
            |_, error| record_hook(kill, error),
        ) {
            Some(Poll::Ready(Ok(()))) | None => Poll::Ready(()),
            Some(Poll::Ready(Err((_error, report)))) => {
                record_hook(kill, report);

                Poll::Ready(())
            }
            Some(Poll::Pending) => {
                if *expired || timer.as_mut().poll(cx).is_ready() {
                    *expired = true;
                    kill.control.report.borrow_mut().callback_timed_out();
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }
    })
    .await;

    contain(|| drop(hook), |error| record_hook(kill, error));
}

fn record_drop<Clock: AktorGroupClock>(
    kill: &AktorKillSwitch<Clock>,
    actor: crate::ActorId,
    name: &str,
    kind: crate::AktorExecution,
    error: String,
) {
    kill.owner_failure(
        actor,
        AktorFailureInput::actor(name.into(), kind, crate::AktorPhase::Drop, error),
    );
}

fn record_hook<Clock: AktorGroupClock>(kill: &AktorKillSwitch<Clock>, error: String) {
    kill.fail(AktorFailureInput::new(
        crate::AktorFailureSource::ShutdownCallback,
        error,
    ));
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
                            core::future::pending::<()>().await
                        })
                        .unwrap();
                }

                let mut listener = group
                    .listen(async move |_| {
                        cleanup.set(cleanup.get() + 1);
                        core::future::pending::<()>().await
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

                assert_eq!(report.ran_past_grace(), draining);
                assert!(report.failed());
                assert_eq!(report.actors[0].ran_past_grace(), draining);
                assert_eq!((cleaned.get(), notified.get()), (1, 1));
                let mut published = Box::pin(completed.wait());
                assert!(
                    matches!(published.as_mut().poll(&mut cx), Poll::Ready(Ok(report)) if report.failed())
                );
            }
        }
    }
}
