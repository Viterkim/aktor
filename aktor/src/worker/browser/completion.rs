use super::*;

impl Completion {
    pub fn new_observer(&self) -> Self {
        Self {
            result: self.result.clone(),
        }
    }

    pub async fn wait(&mut self) -> Result<(), WorkerError> {
        observe(self.result.clone()).await
    }
}

pub async fn observe(
    mut result: watch::Receiver<Option<Result<(), WorkerError>>>,
) -> Result<(), WorkerError> {
    loop {
        if let Some(result) = result.borrow().clone() {
            return result;
        }

        if result.changed().await.is_err() {
            return Err(WorkerError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Closed,
            ));
        }
    }
}
