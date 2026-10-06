use super::*;

impl<S> Listener<S> {
    /// Discard queued work, catching each payload's destructor separately.
    pub fn discard(mut self, failures: &mut Failures) -> watch::Sender<()> {
        failures.capture(FailureKind::Teardown, || self.admission.shutdown());
        failures.capture(FailureKind::Teardown, || self.receiver.close());

        while let Ok(message) = self.receiver.try_recv() {
            failures.capture(FailureKind::Teardown, || drop(message));
        }

        let Self {
            receiver,
            hooks,
            failure,
            admission,
            name,
            handles,
            _finished,
        } = self;

        failures.capture(FailureKind::Teardown, || drop(receiver));
        failures.capture(FailureKind::Teardown, || drop(hooks.before_each));
        failures.capture(FailureKind::Teardown, || drop(hooks.after_each));
        failures.capture(FailureKind::Teardown, || {
            drop((failure, admission, name, handles))
        });

        _finished
    }
}
