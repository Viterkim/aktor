use super::impls::stopped;
use super::shutdown::{contain_drop, finish_hook, panic_message};
use super::*;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

impl AktorGroup {
    /// Your last closure gets the reports after actor cleanup.
    /// Dropping this future cancels that closure, actor cleanup still publishes completion.
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
                application: vec![error],
                ..ShutdownReport::default()
            }));
        }

        let kill = self.killswitch();
        let completed = self.completion();
        let (hook_start, hook_report) = tokio::sync::oneshot::channel();
        let (hook_done, hook_result) = tokio::sync::oneshot::channel();
        let group = Self {
            stop_on_drop: false,
            control: self.control.clone(),
            actors: self.actors.clone(),
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

        self.stop_on_drop = true;
        let mut stopping = self.control.changed.subscribe();
        let mut output = None;

        {
            let mut application =
                Box::pin(AssertUnwindSafe(async { application(&mut self).await }).catch_unwind());
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
                        kill.fail(ActorFailure {
                            actor: "application".into(), phase: "run".into(), message: panic_message(&payload),
                        });
                        contain_drop(payload, &kill, "application panic drop");
                    }
                },
            }

            contain_drop(application, &kill, "application drop");
        }

        kill.stop();
        if let Ok(report) = hook_report.await {
            let reserve = (self.control.grace / 10).min(Duration::from_millis(100));
            let deadline = kill.deadline();
            let hook_deadline = deadline.checked_sub(reserve / 2).unwrap_or(deadline);
            finish_hook(cleanup, report, &kill, hook_deadline).await;
        } else {
            contain_drop(cleanup, &kill, "cleanup drop");
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
