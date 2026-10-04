use super::driver::poll_owners;
use super::*;

impl AktorGroup {
    pub async fn run<O, A, Application, Cleanup, CleanupFuture, E>(
        mut self,
        application: Application,
        cleanup: Cleanup,
    ) -> Result<Option<O>, Box<ShutdownReport>>
    where
        Application: AsyncFnOnce(&mut AktorGroup) -> Result<O, AktorError<A>>,
        Cleanup: FnOnce(ShutdownReport) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
    {
        if let Err(error) = self.claim_listener() {
            return Err(Box::new(ShutdownReport {
                application: alloc::vec![error],
                ..ShutdownReport::default()
            }));
        }
        let driver = Driver {
            control: self.control.clone(),
            owners: self.owners.clone(),
        };
        let owners = self.owners.clone();
        let kill = self.killswitch();
        let changed = kill.control.changed.listen();
        let output = {
            let mut app = Box::pin(application(&mut self));
            poll_fn(|cx| {
                changed.register(cx);
                poll_owners(&owners, cx, &kill.control);
                if kill.is_stopping() {
                    return Poll::Ready(None);
                }
                match app.as_mut().poll(cx) {
                    Poll::Ready(Ok(value)) => Poll::Ready(Some(value)),
                    Poll::Ready(Err(error)) => {
                        kill.control
                            .report
                            .borrow_mut()
                            .application
                            .push(error.report());
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                }
            })
            .await
        };
        let report = self.finish(cleanup).await;
        drop(driver);
        if report.failed() {
            Err(Box::new(report))
        } else {
            Ok(output)
        }
    }
}
