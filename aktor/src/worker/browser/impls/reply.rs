use super::super::*;
use core::{
    pin::Pin,
    task::{Context, Poll},
};

impl<O: DeserializeOwned> WorkerReply<O> {
    /// Take a ready output once. A pending reply can still be awaited.
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken {
            return None;
        }
        match Pin::new(self).poll(&mut Context::from_waker(core::task::Waker::noop())) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        }
    }

    pub(super) fn poll_result(&mut self, context: &mut Context<'_>) -> Poll<Result<O, WireError>> {
        match Pin::new(&mut self.response).poll(context) {
            Poll::Ready(Ok(Ok(output))) => {
                return Poll::Ready(decode(&output).map_err(|error| {
                    let mut error: WireError = error.without_data();
                    error.outcome = CallError::OutcomeUnknown;
                    error
                }));
            }
            Poll::Ready(Ok(Err(error))) => return Poll::Ready(Err(error)),
            Poll::Ready(Err(_)) => {
                return Poll::Ready(Err(WireError::new(
                    CallError::OutcomeUnknown,
                    WorkerCause::Closed,
                )));
            }
            Poll::Pending => {}
        }

        Poll::Pending
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
impl<O: DeserializeOwned> Future for WorkerReply<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();
        if this.parked {
            return Poll::Pending;
        }
        match this.poll_result(context) {
            Poll::Ready(Ok(output)) => {
                this.taken = true;
                Poll::Ready(output)
            }
            Poll::Ready(Err(error)) => {
                this.parked = true;
                if let Some(inner) = this.inner.upgrade() {
                    inner.lost(error)
                } else if let Some((name, group)) = &this.group {
                    group.fail(crate::ActorFailure {
                        actor: name.clone(),
                        phase: "worker".into(),
                        message: error.to_string(),
                    });
                    Poll::Pending
                } else {
                    fatal(error)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
