use super::driver::{capture, contain, poll_owners};
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

        let entries = core::mem::take(&mut *owners.borrow_mut());

        for entry in entries.iter().rev() {
            if let Some(error) = contain(|| (entry.shutdown)()) {
                kill.control.report.borrow_mut().application.push(error);
            }
        }

        *owners.borrow_mut() = entries;

        let deadline = kill
            .control
            .deadline
            .get()
            .unwrap_or_else(|| Clock::deadline(Duration::ZERO));

        let mut timer = Clock::wait(deadline);
        let mut expired = false;

        poll_fn(|cx| {
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
                    record_hook(&kill, "cleanup error drop", error);
                }

                Poll::Ready(())
            }
            Err(error) => {
                record_hook(&kill, "cleanup", error);
                Poll::Ready(())
            }
            Ok(Poll::Pending) => {
                if expired || timer.as_mut().poll(cx).is_ready() {
                    kill.control.report.borrow_mut().timed_out = true;
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        })
        .await;

        if let Some(error) = contain(|| drop(hook)) {
            record_hook(&kill, "cleanup drop", error);
        }

        let mut report = kill.control.report.borrow().clone();

        report.failure = kill.control.failure.borrow().clone();
        *kill.control.completed.borrow_mut() = Some(report.clone());
        kill.control.changed.notify();
        report
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
