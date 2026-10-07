use super::driver::{capture, contain, poll_owners};
use super::*;
use crate::AktorShutdownOutput;
use alloc::string::ToString;
use core::{pin::Pin, task::Context};

impl<Clock: AktorGroupClock> AktorGroup<Clock> {
    pub async fn run<O, A, Application, Cleanup, Output, Mode, E>(
        mut self,
        application: Application,
        cleanup: Cleanup,
    ) -> Result<Option<O>, Box<ShutdownReport>>
    where
        Application: AsyncFnOnce(&mut AktorGroup<Clock>) -> Result<O, AktorError<A>>,
        Cleanup: FnOnce(ShutdownReport) -> Output,
        Output: AktorShutdownOutput<Mode, E>,
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
            let mut app = OwnedApplication {
                future: Some(Box::pin(async { application(&mut self).await })),
                kill: kill.clone(),
            };

            let output = poll_fn(|cx| {
                changed.register(cx);
                poll_owners(&owners, cx, &kill.control);

                if kill.is_stopping() {
                    return Poll::Ready(None);
                }

                match capture(|| Pin::new(&mut app).poll(cx)) {
                    Ok(Poll::Ready(Ok(value))) => Poll::Ready(Some(value)),
                    Ok(Poll::Ready(Err(error))) => {
                        kill.control
                            .report
                            .borrow_mut()
                            .application
                            .push(error.report());

                        if let Some(error) = contain(|| drop(error)) {
                            record_failure(&kill, "application error drop", error);
                        }

                        Poll::Ready(None)
                    }
                    Err(error) => {
                        record_failure(&kill, "run", error);
                        Poll::Ready(None)
                    }
                    Ok(Poll::Pending) => Poll::Pending,
                }
            })
            .await;

            if let Some(error) = contain(|| drop(app)) {
                record_failure(&kill, "application drop", error);
            }

            output
        };

        let report = self
            .finish(move |report| cleanup(report).into_shutdown())
            .await;

        drop(driver);

        if report.failed() {
            Err(Box::new(report))
        } else {
            Ok(output)
        }
    }
}

struct OwnedApplication<F, Clock: AktorGroupClock> {
    future: Option<Pin<Box<F>>>,
    kill: KillSwitch<Clock>,
}
impl<F, Clock: AktorGroupClock> Drop for OwnedApplication<F, Clock> {
    fn drop(&mut self) {
        if let Some(error) = contain(|| drop(self.future.take())) {
            record_failure(&self.kill, "application drop", error);
        }
    }
}
impl<F: Future, Clock: AktorGroupClock> Future for OwnedApplication<F, Clock> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().future.as_mut() {
            Some(future) => future.as_mut().poll(cx),
            None => Poll::Pending,
        }
    }
}

fn record_failure<Clock: AktorGroupClock>(
    kill: &KillSwitch<Clock>,
    phase: &str,
    error: AktorError,
) {
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
    use core::task::{Context, Waker};

    struct Clock;
    impl AktorGroupClock for Clock {
        type Deadline = ();

        fn deadline(_: Duration) {}

        fn wait(_: ()) -> LocalFuture<'static, ()> {
            Box::pin(core::future::pending())
        }
    }

    #[test]
    fn panic_cleanup() {
        for actor_panic in [false, true] {
            let cleaned = Rc::new(Cell::new(0));
            let cleanup = cleaned.clone();
            let notified = Rc::new(Cell::new(0));
            let notify = notified.clone();
            let group = AktorGroup::<Clock>::new();
            let completed = group.completion();
            group
                .on_shutdown(move |_| notify.set(cleaned.get()))
                .unwrap();

            let mut run = Box::pin(group.run(
                async move |group| -> Result<(), AktorError> {
                    let handle = group
                        .spawn::<(), 1, ()>(ActorArgs {
                            name: "custom clock actor".into(),
                            capacity: 1,
                            setup: async || Ok(()),
                            cleanup: async move |_| {
                                let mut yielded = false;
                                poll_fn(|cx| {
                                    if yielded {
                                        Poll::Ready(())
                                    } else {
                                        yielded = true;
                                        cx.waker().wake_by_ref();
                                        Poll::Pending
                                    }
                                })
                                .await;
                                cleanup.set(cleanup.get() + 1);
                                Ok(())
                            },
                        })
                        .unwrap();

                    handle.ready().await.unwrap();
                    if actor_panic {
                        let operation = crate::operation::Operation {
                            name: "setup",
                            caller: std::panic::Location::caller(),
                        };
                        assert_eq!(
                            Request::new(
                                &handle,
                                operation,
                                async |_, ()| { Err::<(), _>("query error") },
                                ()
                            )
                            .await,
                            Err("query error")
                        );
                        assert!(!group.killswitch().is_stopping());
                        Request::new(
                            &handle,
                            operation,
                            async |_, ()| {
                                panic!("custom clock actor failed");
                            },
                            (),
                        )
                        .await;
                        panic!("application continued after actor failure");
                    }
                    panic!("custom clock application failed");
                },
                async |_| Ok::<_, AktorError>(()),
            ));
            let mut cx = Context::from_waker(Waker::noop());
            let mut report = None;

            for _ in 0..32 {
                if let Poll::Ready(result) = run.as_mut().poll(&mut cx) {
                    report = Some(result.unwrap_err());
                    break;
                }
            }

            let report = report.expect("local cleanup did not finish");
            assert!(!report.startup);
            assert_eq!(notified.get(), 1, "{report}");
            assert_eq!(
                report.failure.unwrap().message,
                if actor_panic {
                    "custom clock actor failed"
                } else {
                    "custom clock application failed"
                }
            );
            let mut published = Box::pin(completed.wait());
            assert!(
                matches!(published.as_mut().poll(&mut cx), Poll::Ready(report) if report.failed())
            );
        }
    }
}
