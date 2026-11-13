use super::*;
use crate::{AktorCleanupOutput, AktorError};

/// Wait for the page's external work before stopping the worker, even if startup was cancelled.
/// Return once the work has stopped. An error from the join lets shutdown continue too.
pub fn join<Output: AktorCleanupOutput<Mode>, Mode>(
    joining: impl Future<Output = Output> + 'static,
) -> WorkerJoin {
    let (finished, receipt) = watch::channel(None);
    let joining = Box::pin(async move {
        let result = joining.await.into_cleanup();
        finished.send_replace(Some(result.clone()));
        result
    });

    WorkerJoin {
        joining: Some(joining),
        receipt,
    }
}

pub(super) fn reported<Output: AktorCleanupOutput<Mode>, Mode>(
    joining: impl Future<Output = Output> + 'static,
) -> LocalFuture<'static, Result<(), AktorError>> {
    Box::pin(async move { joining.await.into_cleanup() })
}

impl WorkerJoin {
    pub(crate) fn completion(&self) -> watch::Receiver<Option<Result<(), AktorError>>> {
        self.receipt.clone()
    }

    pub(crate) fn attach<S: 'static, Role: 'static, E: 'static>(
        mut self,
        worker: Worker<S, Role, E>,
    ) -> Result<Worker<S, Role, E>, WorkerFailure> {
        match self.joining.take() {
            Some(joining) => worker.attach_shutdown(joining),
            None => Ok(worker),
        }
    }
}
impl Default for WorkerJoin {
    fn default() -> Self {
        Self {
            joining: None,
            receipt: watch::channel(Some(Ok(()))).1,
        }
    }
}
impl Drop for WorkerJoin {
    fn drop(&mut self) {
        let Some(joining) = self.joining.take() else {
            return;
        };

        // Construction can fail before a worker exists to own this join.
        wasm_bindgen_futures::spawn_local(async move {
            let _result = joining.await;
        });
    }
}
