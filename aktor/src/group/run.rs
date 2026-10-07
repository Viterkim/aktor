use super::impls::stopped;
use super::shutdown::{contain_drop, fail_application, finish_hook, panic_message};
use super::*;
use crate::AktorShutdownOutput;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

impl AktorGroup {
    /// Your last closure gets the reports after actor cleanup.
    /// Dropping this future cancels that closure, actor cleanup still publishes completion.
    pub fn run<O, A, Application, Cleanup, Output, Mode, E>(
        self,
        application: Application,
        cleanup: Cleanup,
    ) -> impl Future<Output = Result<Option<O>, Box<ShutdownReport>>>
    where
        Application: AsyncFnOnce(&mut AktorGroup) -> Result<O, AktorError<A>>,
        Cleanup: FnOnce(ShutdownReport) -> Output,
        Output: AktorShutdownOutput<Mode, E>,
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
            if let Err(error) = owner.claim_listener() {
                return Err(Box::new(ShutdownReport {
                    application: vec![error],
                    ..ShutdownReport::default()
                }));
            }

            let kill = owner.killswitch();
            owner.control.lock().applications += 1;
            factory.life = Some(ApplicationLife {
                kill: kill.clone(),
                stop_on_drop: true,
            });
            let completed = owner.completion();
            let (hook_start, hook_report) = tokio::sync::oneshot::channel();
            let (hook_done, hook_result) = tokio::sync::oneshot::channel();
            let group = Self {
                stop_on_drop: false,
                control: owner.control.clone(),
                startup_failure: owner.startup_failure.clone(),
                actors: owner.actors.clone(),
                #[cfg(target_family = "wasm")]
                shutdown_hook: owner.shutdown_hook.clone(),
            };

            let closing = async move {
                group.killswitch().wait_stopping().await;
                group
                    .finish(async move |report| {
                        let _sent = hook_start.send(report);

                        hook_result.await.map_err(|_| {
                            AktorError::new("group driver cancelled before cleanup finished")
                        })
                    })
                    .await;
            };

            #[cfg(not(target_family = "wasm"))]
            tokio::spawn(closing);
            #[cfg(target_family = "wasm")]
            wasm_bindgen_futures::spawn_local(closing);

            owner.stop_on_drop = true;

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
                            kill.control.lock().report.application.push(error.report());
                            contain_drop(error, &kill, "application error drop");
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

            if let Ok(report) = hook_report.await {
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
