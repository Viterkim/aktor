use super::*;

struct Running<S> {
    state: Option<S>,
    listener: Option<Listener<S>>,
}
impl<S> Running<S> {
    async fn run(mut self) -> S {
        let Some(listener) = self.listener.as_mut() else {
            crate::message::consumed();
        };

        let policy = listener.failure.clone();
        let mut failures = Failures::new(listener.name.clone());

        while let Some(message) = listener.receiver.recv().await {
            if let Some(state) = self.state.as_mut()
                && failures
                    .capture_async(
                        FailureKind::Runtime,
                        message.run_with(state, &mut listener.hooks),
                    )
                    .await
                    .is_none()
            {
                break;
            }
        }

        failures.capture(FailureKind::Teardown, || listener.receiver.close());

        if failures.first.is_some() {
            failures.capture(FailureKind::Cleanup, || drop(self.state.take()));
        }

        if let Some(listener) = self.listener.take() {
            let finished = listener.discard(&mut failures);
            failures.capture(FailureKind::Teardown, || drop(finished));
        }

        failures.finish(&policy);

        match self.state.take() {
            Some(state) => state,
            None => crate::message::consumed(),
        }
    }
}
impl<S> Drop for Running<S> {
    fn drop(&mut self) {
        let Some(mut listener) = self.listener.take() else {
            return;
        };

        let policy = listener.failure.clone();
        let mut failures = Failures::new(listener.name.clone());

        failures.first = Some(Failure {
            actor: listener.name.clone(),
            kind: FailureKind::Cancelled,
            payload: Box::new("actor task cancelled before completion"),
        });

        failures.capture(FailureKind::Teardown, || listener.receiver.close());
        failures.capture(FailureKind::Cleanup, || drop(self.state.take()));

        let finished = listener.discard(&mut failures);

        failures.capture(FailureKind::Teardown, || drop(finished));

        if let Some(failure) = failures.first {
            if std::thread::panicking() {
                policy.report(&failure);
            } else {
                policy.fail(failure);
            }
        }
    }
}

pub fn run<S>(listener: Listener<S>, state: S) -> impl Future<Output = S> {
    Running {
        state: Some(state),
        listener: Some(listener),
    }
    .run()
}
