use super::*;
use crate::AktorError;

impl ShutdownDependency {
    pub fn new(joining: Option<LocalFuture<'static, Result<(), AktorError>>>) -> Self {
        let initial = joining.is_none().then_some(Ok(()));
        let (finished, _) = watch::channel(initial);

        Self { joining, finished }
    }

    pub fn completion(&self) -> watch::Receiver<Option<Result<(), AktorError>>> {
        self.finished.subscribe()
    }

    pub fn attach<S: 'static, Role: 'static, E: 'static>(
        self,
        worker: Worker<S, Role, E>,
    ) -> Result<Worker<S, Role, E>, WorkerError> {
        if self.joining.is_none() {
            return Ok(worker);
        }

        worker.before_shutdown(self.wait())
    }

    async fn wait(self) -> Result<(), AktorError> {
        let mut result = self.completion();
        drop(self);

        loop {
            if let Some(result) = result.borrow_and_update().clone() {
                return result;
            }

            result.changed().await.map_err(|_| {
                AktorError::new("external shutdown join stopped without a receipt", ())
            })?;
        }
    }
}
impl Drop for ShutdownDependency {
    fn drop(&mut self) {
        let Some(joining) = self.joining.take() else {
            return;
        };
        let finished = self.finished.clone();

        // Construction can fail before a worker exists to own this join.
        wasm_bindgen_futures::spawn_local(async move {
            let result = joining.await;
            finished.send_replace(Some(result));
        });
    }
}
