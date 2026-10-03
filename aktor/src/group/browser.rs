use super::*;
use crate::worker::{Options, Worker, WorkerCause, WorkerError};

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
        let worker = Worker::<S, Role, E>::with_options(url, options)
            .map_err(|error| error.without_data())?;
        worker.manage(name.clone(), self.killswitch());
        let shutdown = worker.new_handle();
        let cancel = worker.new_handle();
        let completion = worker.completion();
        let label = name.clone();
        self.actors.push(Entry {
            name,
            start: Box::new(move || {
                shutdown.shutdown();
            }),
            cancel: Box::new(move || cancel.terminate()),
            outcome: Box::pin(async move {
                let mut outcome = ActorOutcome {
                    actor: label,
                    diagnostics: Vec::new(),
                    timed_out: false,
                };
                if let Err(error) = completion.wait_report().await {
                    match error.cause {
                        WorkerCause::Cleanup(message) => {
                            outcome.diagnostics.push(AktorCleanupError {
                                diagnostics: message,
                                data: (),
                            })
                        }
                        WorkerCause::Closed | WorkerCause::Timeout => outcome.timed_out = true,
                        _ => {}
                    }
                }
                outcome
            }),
        });
        worker.ready().await?;
        Ok(worker)
    }
}
