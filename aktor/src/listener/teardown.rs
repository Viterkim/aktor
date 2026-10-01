use super::*;

impl<S> Listener<S> {
    /// Discard queued work, catching each payload's destructor separately.
    pub fn discard(mut self, failures: &mut Failures) -> watch::Sender<()> {
        self.close();
        while let Ok(message) = self.receiver.try_recv() {
            failures.capture(FailureKind::Teardown, || drop(message));
        }

        let Self {
            receiver,
            failure,
            admission,
            name,
            handles,
            _finished,
        } = self;
        failures.capture(FailureKind::Teardown, || drop(receiver));
        failures.capture(FailureKind::Teardown, || {
            drop((failure, admission, name, handles))
        });

        _finished
    }
}
