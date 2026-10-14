use super::*;
use crate::worker::{Options, Worker, WorkerCause, WorkerError};
use core::cell::Cell;

impl AktorGroup {
    /// Open a worker and keep it with the group.
    pub async fn worker<S: 'static>(
        &mut self,
        name: impl Into<String>,
        url: &str,
        options: Options,
    ) -> Result<Worker<S>, WorkerError> {
        self.worker_for::<(), S>(name, url, options).await
    }

    /// Use the marker from #[aktor(actor = ...)].
    pub async fn worker_for<Role: 'static, S: 'static>(
        &mut self,
        name: impl Into<String>,
        url: &str,
        options: Options,
    ) -> Result<Worker<S, Role>, WorkerError> {
        self.worker_for_data::<Role, S, ()>(name, url, options)
            .await
    }

    pub async fn worker_for_data<
        Role: 'static,
        S: 'static,
        E: serde::de::DeserializeOwned + 'static,
    >(
        &mut self,
        name: impl Into<String>,
        url: &str,
        options: Options,
    ) -> Result<Worker<S, Role, E>, WorkerError<E>> {
        let name = name.into();

        if !self.control.lock().listening {
            return Err(WorkerError {
                outcome: crate::message::CallError::NotAdmitted,
                cause: WorkerCause::Setup("start the actor group before opening workers".into()),
                data: None,
            });
        }

        if self.killswitch().is_stopping() {
            self.completion().wait().await;
            return Err(WorkerError {
                outcome: crate::message::CallError::NotAdmitted,
                cause: WorkerCause::Closed,
                data: None,
            });
        }

        self.control
            .lock()
            .kinds
            .push((name.clone(), AktorExecution::BrowserWebWorker));

        let opened = Worker::<S, Role, E>::with_options(url, options);
        let worker = match opened {
            Ok(worker) => worker,
            Err(error) => {
                self.killswitch().fail_startup(ActorFailure {
                    kind: None,
                    actor: name,
                    phase: "setup".into(),
                    message: format!("{error:#}"),
                });

                self.completion().wait().await;
                return Err(error.without_data());
            }
        };

        {
            let mut entries = self.actors.borrow_mut();

            worker.manage(name.clone(), self.killswitch());

            let shutdown = worker.new_handle();
            let cancel = worker.new_handle();
            let completion = worker.completion();
            let label = name.clone();
            let timed_out = Rc::new(Cell::new(false));
            let forced = timed_out.clone();

            entries.push(Entry {
                kind: AktorExecution::BrowserWebWorker,
                name,
                start: Box::new(move || {
                    shutdown.shutdown();
                }),
                cancel: Box::new(move || {
                    forced.set(true);
                    cancel.terminate();
                }),
                outcome: Box::pin(async move {
                    let mut outcome = ActorOutcome {
                        kind: None,
                        actor: label,
                        diagnostics: Vec::new(),
                        timed_out: false,
                    };

                    outcome.diagnostics = completion.wait_diagnostics().await;
                    outcome.timed_out = timed_out.get();

                    outcome
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
