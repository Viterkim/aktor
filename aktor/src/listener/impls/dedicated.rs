use super::super::*;
use tokio::task;

impl<S> Dedicated<S> {
    pub fn completion(&self) -> CompletionObserver {
        CompletionObserver {
            finished: self.finished.clone(),
        }
    }

    pub fn join(self) -> Result<S, DedicatedJoinError> {
        match self.thread.join() {
            Ok(Some(state)) => Ok(state),
            Err(payload) => Err(DedicatedJoinError {
                payload: parking_lot::Mutex::new(payload),
            }),
            Ok(None) => Err(DedicatedJoinError {
                payload: parking_lot::Mutex::new(Box::new("actor thread returned no output")),
            }),
        }
    }

    /// Join without blocking the async runtime.
    pub async fn join_async(self) -> Result<S, DedicatedJoinError>
    where
        S: Send + 'static,
    {
        self.completion().wait().await;

        task::spawn_blocking(move || self.join())
            .await
            .map_err(|error| DedicatedJoinError {
                payload: parking_lot::Mutex::new(if error.is_panic() {
                    error.into_panic()
                } else {
                    Box::new(error)
                }),
            })?
    }

    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }
}

impl CompletionObserver {
    pub async fn wait(&mut self) {
        let _result = self.finished.changed().await;
    }

    pub fn is_complete(&self) -> bool {
        self.finished.has_changed().is_err()
    }
}
