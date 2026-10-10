use super::impls::stopped;
use super::shutdown::{contain_drop, fail_application, finish_hook, panic_message};
use super::*;
use crate::AktorShutdownOutput;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

impl AktorGroup {
    /// Your last closure gets the reports after actor cleanup.
    /// Dropping this future cancels that closure, actor cleanup still publishes completion.
    pub fn run<O, Application, Cleanup, Output, Mode>(
        self,
        application: Application,
        cleanup: Cleanup,
    ) -> impl Future<Output = Result<Option<O>, Box<ShutdownReport>>>
    where
        Application: AsyncFnOnce(&mut AktorGroup) -> Result<O, AktorError>,
        Cleanup: FnOnce(ShutdownReport) -> Output,
        Output: AktorShutdownOutput<Mode>,
    {
        let kill = self.killswitch();
        let mut factory = ApplicationFactory {
            owner: Some(self),
            application: Some(application),
            cleanup: Some(cleanup),
            kill,
            life: None,
        };
        async move {
            let Some(mut owner) = factory.owner.take() else {
                crate::message::consumed()
            };
            let (hook_start, hook_report) = tokio::sync::oneshot::channel();
            let (hook_done, hook_result) = tokio::sync::oneshot::channel();
            #[cfg(not(target_family = "wasm"))]
            let hook_kill = owner.killswitch();
            let completed = match owner.start_with(async move |report| {
                // Keep the watchdog watching while cleanup runs on the caller.
                #[cfg(not(target_family = "wasm"))]
                let lifetime = hook_kill.track_thread("application cleanup".into());
                #[cfg(target_family = "wasm")]
                let lifetime = ();
                let _sent = hook_start.send((report, lifetime));

                hook_result
                    .await
                    .map_err(|_| AktorError::new("group driver cancelled before cleanup finished"))
            }) {
                Ok(completed) => completed,
                Err(error) => {
                    return Err(Box::new(ShutdownReport {
                        application: vec![error],
                        ..ShutdownReport::default()
                    }));
                }
            };

            let kill = owner.killswitch();

            owner.control.lock().applications += 1;
            factory.life = Some(ApplicationLife {
                kill: kill.clone(),
                stop_on_drop: true,
            });

            let mut stopping = owner.control.changed.subscribe();
            let mut output = None;

            {
                let mut application = OwnedApplication {
                    future: Some(Box::pin(
                        AssertUnwindSafe(async {
                            let Some(application) = factory.application.take() else {
                                crate::message::consumed()
                            };
                            application(&mut owner).await
                        })
                        .catch_unwind(),
                    )),
                    kill: kill.clone(),
                };

                tokio::select! {
                    biased;
                    _ = stopped(&mut stopping) => {}
                    value = &mut application => match value {
                        Ok(Ok(value)) => output = Some(value),
                        Ok(Err(error)) => {
                            kill.stop();
                            kill.control.lock().report.application.push(error);
                        }
                        Err(payload) => {
                            fail_application(&kill, "run", panic_message(&payload));
                            contain_drop(payload, &kill, "application panic drop");
                        }
                    },
                }

                contain_drop(application, &kill, "application drop");
            }

            kill.stop();
            drop(factory.life.take());

            if let Ok((report, _lifetime)) = hook_report.await {
                let reserve = (owner.control.grace / 10).min(Duration::from_millis(100));
                let deadline = kill.deadline();
                let hook_deadline = deadline.checked_sub(reserve / 2).unwrap_or(deadline);

                let Some(cleanup) = factory.cleanup.take() else {
                    crate::message::consumed()
                };
                finish_hook(
                    move |report| cleanup(report).into_shutdown(),
                    report,
                    &kill,
                    hook_deadline,
                )
                .await;
            } else {
                contain_drop(factory.cleanup.take(), &kill, "cleanup drop");
            }

            let _sent = hook_done.send(());
            let report = completed.wait().await;

            if report.failed() {
                Err(Box::new(report))
            } else {
                Ok(output)
            }
        }
    }
}
