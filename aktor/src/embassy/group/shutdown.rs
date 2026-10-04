use super::driver::poll_owners;
use super::*;

impl AktorGroup {
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
            (entry.shutdown)();
        }
        *owners.borrow_mut() = entries;
        let mut timer = Box::pin(embassy_time::Timer::at(
            kill.control
                .deadline
                .get()
                .unwrap_or_else(embassy_time::Instant::now),
        ));
        poll_fn(|cx| {
            poll_owners(&owners, cx, &kill.control);
            if owners.borrow().iter().all(|entry| entry.finished) {
                return Poll::Ready(());
            }
            if timer.as_mut().poll(cx).is_ready() {
                kill.control.report.borrow_mut().timed_out = true;
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await;
        let entries = core::mem::take(&mut *owners.borrow_mut());
        for entry in entries.iter().filter(|entry| !entry.finished) {
            kill.control.report.borrow_mut().actors.push(ActorOutcome {
                actor: entry.name.clone(),
                diagnostics: Vec::new(),
                timed_out: true,
            });
        }
        drop(entries);
        let mut report = kill.control.report.borrow().clone();
        report.failure = kill.control.failure.borrow().clone();
        let mut hook = Box::pin(cleanup(report.clone()));
        poll_fn(|cx| match hook.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(error)) => {
                let error = error.report();
                kill.control
                    .report
                    .borrow_mut()
                    .application
                    .push(error.clone());
                report.application.push(error);
                Poll::Ready(())
            }
            Poll::Pending => {
                if timer.as_mut().poll(cx).is_ready() {
                    report.timed_out = true;
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        })
        .await;
        drop(hook);
        report.failure = kill.control.failure.borrow().clone();
        *kill.control.completed.borrow_mut() = Some(report.clone());
        kill.control.changed.notify();
        report
    }
}
