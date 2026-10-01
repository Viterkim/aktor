use super::super::*;
use wasm_bindgen::JsValue;

impl Inner {
    pub fn fail(&self, cause: WorkerCause) {
        self.closed.set(true);
        self.count.close();
        self.bytes.close();

        if self.ready.borrow().is_none() {
            self.ready.send_replace(Some(Err(WorkerError::new(
                CallError::NotAdmitted,
                cause.clone(),
            ))));
        }

        for (_, work) in self.outstanding.replace(HashMap::new()) {
            let outcome = if work.input.is_some() {
                CallError::Discarded
            } else {
                CallError::OutcomeUnknown
            };

            if let Some(answer) = work.answer {
                let _sent = answer.send(Err(WorkerError::new(outcome, cause.clone())));
            }
        }

        self.queue.borrow_mut().clear();
        self.active.set(None);
        self.executing.set(false);
        self.finish(Err(WorkerError::new(CallError::OutcomeUnknown, cause)));
    }

    pub fn finish(&self, result: Result<(), WorkerError>) {
        if self.finished.borrow().is_none() {
            self.finished.send_replace(Some(result));
        }

        self.worker.terminate();
        self.retained.borrow_mut().take();
    }

    pub fn pump(&self) {
        if self.active.get().is_some()
            || !matches!(*self.ready.borrow(), Some(Ok(())))
            || self.finished.borrow().is_some()
        {
            return;
        }

        let next = self.queue.borrow_mut().pop_front();
        if let Some(id) = next {
            let input = self
                .outstanding
                .borrow_mut()
                .get_mut(&id)
                .and_then(|work| work.input.take());

            if let Some(input) = input {
                self.active.set(Some(id));
                if let Err(error) = post(&self.worker, &input) {
                    self.fail(error.cause);
                }
            }
        } else if self.closed.get()
            && !self.shutdown_sent.replace(true)
            && let Err(error) = post(&self.worker, &Incoming::Shutdown)
        {
            self.fail(error.cause);
        }
    }

    pub fn shutdown(self: &Rc<Self>) {
        if self.closed.replace(true) {
            return;
        }

        self.count.close();
        self.bytes.close();

        // Keep the worker around until accepted work and cleanup finish.
        *self.retained.borrow_mut() = Some(self.clone());
        let inner = self.clone();

        wasm_bindgen_futures::spawn_local(async move {
            match observe(inner.ready.subscribe()).await {
                Ok(()) => inner.pump(),
                Err(error) => inner.fail(error.cause),
            }
        });
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.worker.set_onmessage(None);
        self.worker.set_onerror(None);
        self.worker.terminate();
    }
}

fn post(worker: &web_sys::Worker, input: &Incoming) -> Result<(), WorkerError> {
    let input = encode(input)?;

    worker
        .post_message(&JsValue::from_str(&input))
        .map_err(|error| {
            WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Codec(format!("{error:?}")),
            )
        })
}
