use super::super::*;
use core::{
    pin::Pin,
    task::{Context, Poll},
};

impl<O> WorkerReply<O> {
    pub(in crate::worker::browser) fn poll_outcome(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<O, crate::AktorDeliveryError>> {
        if self.taken {
            crate::message::consumed();
        }

        let received = core::task::ready!(Pin::new(&mut self.response).poll(cx));
        Poll::Ready(self.finish(received.ok()))
    }

    fn finish(
        &mut self,
        received: Option<Result<O, WireFailure>>,
    ) -> Result<O, crate::AktorDeliveryError> {
        let result = received.unwrap_or_else(|| {
            Err(self
                .inner
                .upgrade()
                .and_then(|inner| inner.recorded_failure(AktorDeliveryOutcome::OutcomeUnknown))
                .unwrap_or_else(|| {
                    WireFailure::new(AktorDeliveryOutcome::OutcomeUnknown, WorkerCause::Closed)
                }))
        });
        self.taken = true;
        if let Err(error) = &result {
            deliver(error.error.failure_id, &self.group);
        }
        Ok(result?)
    }

    /// Take a ready output once. A pending reply can still be awaited.
    pub fn try_take(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        if self.taken {
            return None;
        }
        match self.response.try_recv() {
            Ok(value) => Some(self.finish(Some(value))),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(self.finish(None)),
        }
    }

    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::AktorTimeout<&mut Self> {
        crate::AktorTimeout::browser(self, duration, |_| crate::timeout::WaitStatus {
            admitted: true,
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
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_outcome(context)
    }
}
