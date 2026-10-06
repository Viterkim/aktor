use super::super::*;

impl<S> Listener<S> {
    pub async fn recv(&mut self) -> Option<Message<S>> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<Message<S>, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn close(&mut self) {
        self.admission.shutdown();
        self.receiver.close();
    }

    pub fn capacity(&self) -> usize {
        self.receiver.capacity()
    }

    /// Listen on Tokio until all handles are dropped.
    pub fn run(self, state: S) -> impl Future<Output = S> {
        running::run(self, state)
    }

    pub async fn serve(&mut self, state: &mut S) {
        while let Some(message) = self.receiver.recv().await {
            message.run_with(state, &mut self.hooks).await;
        }
    }

    /// Listen on the current thread until all handles are dropped.
    #[cfg(feature = "tokio")]
    pub fn run_blocking(mut self, mut state: S) -> S {
        let policy = self.failure.clone();
        let mut failures = Failures::new(self.name.clone());

        failures.capture(FailureKind::Runtime, || self.serve_blocking(&mut state));

        failures.capture(FailureKind::Teardown, || self.receiver.close());

        let state = if failures.first.is_some() {
            failures.capture(FailureKind::Cleanup, || drop(state));
            None
        } else {
            Some(state)
        };

        let finished = self.discard(&mut failures);

        failures.capture(FailureKind::Teardown, || drop(finished));
        failures.finish(&policy);

        match state {
            Some(state) => state,
            None => crate::message::consumed(),
        }
    }

    #[cfg(feature = "tokio")]
    pub fn serve_blocking(&mut self, state: &mut S) {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => std::panic::resume_unwind(Box::new(error)),
        };

        runtime.block_on(self.serve(state));
    }
}
