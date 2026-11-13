use super::*;
use crate::worker::{Options, Worker, WorkerCause, WorkerFailure, WorkerStartError};
use core::cell::Cell;

impl AktorGroup {
    #[doc(hidden)]
    /// Open a worker and keep it with the group.
    pub async fn worker<S: 'static>(
        &mut self,
        name: impl Into<String>,
        url: &str,
        options: Options,
    ) -> Result<Worker<S>, (WorkerStartError, String)> {
        self.worker_for::<(), S>(name, url, options).await
    }

    #[doc(hidden)]
    /// Use the marker from #[aktor(role = ...)].
    pub async fn worker_for<Role: 'static, S: 'static>(
        &mut self,
        name: impl Into<String>,
        url: &str,
        options: Options,
    ) -> Result<Worker<S, Role>, (WorkerStartError, String)> {
        self.worker_for_data::<Role, S, ()>(name, url, options)
            .await
    }

    #[doc(hidden)]
    pub async fn worker_for_data<
        Role: 'static,
        S: 'static,
        E: serde::de::DeserializeOwned + 'static,
    >(
        &mut self,
        name: impl Into<String>,
        url: &str,
        options: Options,
    ) -> Result<Worker<S, Role, E>, (WorkerStartError<E>, String)> {
        let name = name.into();

        if !self.control.lock().listening {
            return Err(WorkerStartError::not_run(WorkerFailure::setup(
                crate::message::CallError::NotAdmitted,
                "start the actor group before opening workers",
            )));
        }

        if self.killswitch().is_stopping() {
            self.completion().wait().await;
            return Err(WorkerStartError::not_run(WorkerFailure::new(
                crate::message::CallError::NotAdmitted,
                WorkerCause::Closed,
            )));
        }

        self.control
            .lock()
            .kinds
            .push((name.clone(), AktorExecution::BrowserWebWorker));

        let opened = Worker::<S, Role, E>::with_policy(url, options, false);
        let worker = match opened {
            Ok(worker) => worker,
            Err(error) => {
                self.killswitch().fail_startup(AktorFailureReport {
                    kind: None,
                    actor: name,
                    phase: "setup".into(),
                    message: format!("{error:#}"),
                });

                self.completion().wait().await;
                return Err(WorkerStartError::not_run(error));
            }
        };

        {
            let mut entries = self.actors.borrow_mut();

            worker.manage(name.clone(), self.killswitch());

            let shutdown = worker.clone();
            let cancel = worker.clone();
            let owner = worker.clone();
            let label = name.clone();
            let timed_out = Rc::new(Cell::new(false));
            let forced = timed_out.clone();

            entries.push(Entry {
                kind: AktorExecution::BrowserWebWorker,
                name,
                start: Box::new(move || {
                    shutdown.shutdown();
                }),
                // A worker's `end` runs inside it, terminating early would cut it off.
                cancel: Box::new(|| {}),
                give_up: Some(Box::new(move || {
                    forced.set(true);
                    cancel.give_up(crate::AktorStuck::Kill)
                })),
                confirm_later: None,
                outcome: Box::pin(async move {
                    let (retirement, diagnostics) =
                        owner.group_outcome(crate::AktorStuck::Kill).await;

                    AktorOutcome {
                        retirement,
                        kind: AktorExecution::BrowserWebWorker,
                        actor: label,
                        diagnostics,
                        ran_past_grace: timed_out.get(),
                    }
                }),
            });
        }

        if let Err(error) = worker.ready().await {
            self.completion().wait().await;
            return Err(error);
        }

        Ok(worker)
    }
}
