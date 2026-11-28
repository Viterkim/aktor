use super::*;

/// Wait for external work before stopping the worker. An unconfirmed join keeps the worker alive.
pub fn join(joining: impl Future<Output = WorkerJoinOutcome> + 'static) -> WorkerJoin {
    let (finished, receipt) = watch::channel(None);
    let joining = Box::pin(async move {
        let result = joining.await;
        finished.send_replace(Some(result.clone()));
        result
    });

    WorkerJoin {
        joining: Some(joining),
        receipt,
    }
}

impl WorkerJoin {
    pub(crate) fn completion(&self) -> JoinReceipt {
        self.receipt.clone()
    }

    pub(crate) fn attach<C: AktorWorkerContract>(mut self, worker: &Worker<C>) {
        if let Some(joining) = self.joining.take() {
            *worker.inner.shutdown_join.borrow_mut() = ShutdownJoin::Waiting(joining);
        }
    }
}
impl Drop for WorkerJoin {
    fn drop(&mut self) {
        if let Some(joining) = self.joining.take() {
            wasm_bindgen_futures::spawn_local(async move {
                joining.await;
            });
        }
    }
}
