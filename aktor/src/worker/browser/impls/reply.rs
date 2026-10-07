use super::super::*;
use core::{
    pin::Pin,
    task::{Context, Poll},
};

impl<O> WorkerReply<O> {
    /// Take a ready output once. A pending reply can still be awaited.
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken || self.parked {
            return None;
        }

        let result = match self.response.try_recv() {
            Ok(Ok(output)) => self.decode(&output),
            Ok(Err(error)) => Err(error),
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => Err(WireError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Closed,
            )),
        };

        match self.ready(result) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        }
    }

    fn decode(&self, output: &[u8]) -> Result<O, WireError> {
        (self.decoder)(output).map_err(|error| error.without_data())
    }

    fn ready(&mut self, result: Result<O, WireError>) -> Poll<O> {
        match result {
            Ok(output) => {
                self.taken = true;
                Poll::Ready(output)
            }
            Err(error) => {
                self.parked = true;

                if let Some(inner) = self.inner.upgrade() {
                    inner.lost(error)
                } else if let Some((name, group)) = &self.group {
                    group.fail(crate::ActorFailure {
                        kind: None,
                        actor: name.clone(),
                        phase: "worker".into(),
                        message: error.to_string(),
                    });

                    Poll::Pending
                } else {
                    fatal(error)
                }
            }
        }
    }

    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::Timeout<&mut Self> {
        crate::Timeout::browser(self, duration, |reply| crate::timeout::WaitStatus {
            admitted: true,
            stopping: reply
                .group
                .as_ref()
                .is_some_and(|(_, group)| group.is_stopping()),
        })
    }
}
impl<O> WorkerReply<O> {
    fn abandon(&self) {
        if let Some(inner) = self.inner.upgrade() {
            let answer = inner
                .outstanding
                .borrow_mut()
                .get_mut(&self.id)
                .and_then(|work| work.answer.take());

            drop(answer);
        }
    }
}
impl<O> Drop for WorkerReply<O> {
    fn drop(&mut self) {
        self.abandon();
    }
}
impl<O> Future for WorkerReply<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if this.parked {
            return Poll::Pending;
        }

        match this.poll_result(context) {
            Poll::Ready(result) => this.ready(result),
            Poll::Pending => Poll::Pending,
        }
    }
}
