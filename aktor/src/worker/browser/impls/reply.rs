use super::super::*;
use core::{
    pin::Pin,
    task::{Context, Poll},
};

impl<O> WorkerReply<O> {
    /// Receive this call's outcome without waiting for worker cleanup.
    pub async fn receive_settled(mut self) -> Result<O, WorkerError> {
        core::future::poll_fn(|cx| self.poll_outcome(cx)).await
    }

    pub(in crate::worker::browser) fn poll_outcome(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<O, WorkerError>> {
        if let Some(error) = self.error.take() {
            self.taken = true;
            return Poll::Ready(Err(error.without_data()));
        }

        if self.taken {
            crate::message::consumed();
        }

        match Pin::new(&mut self.response).poll(cx) {
            Poll::Ready(result) => {
                self.taken = true;
                Poll::Ready(
                    result
                        .unwrap_or_else(|_| {
                            Err(WireError::new(
                                CallError::OutcomeUnknown,
                                WorkerCause::Closed,
                            ))
                        })
                        .map_err(WireError::without_data),
                )
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// Take a ready output once. A pending reply can still be awaited.
    pub fn try_take(&mut self) -> Option<O> {
        if self.taken || self.parked {
            return None;
        }

        let result = match self.response.try_recv() {
            Ok(output) => output,
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => {
                let _parked = self.missing();
                return None;
            }
        };

        match self.ready(result) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        }
    }

    fn missing(&mut self) -> Poll<O> {
        self.parked = true;
        let error = WireError::new(CallError::OutcomeUnknown, WorkerCause::Closed);

        self.error = Some(error.clone());

        if let Some(inner) = self.inner.upgrade() {
            inner.lost(error)
        } else if let Some((name, group)) = &self.group {
            if group.is_finished() {
                fatal(error);
            }

            group.fail(crate::AktorFailureReport {
                kind: None,
                actor: name.clone(),
                phase: "worker".into(),
                message: format!("{error:#}"),
            });
            Poll::Pending
        } else {
            fatal(error)
        }
    }

    fn ready(&mut self, result: Result<O, WireError>) -> Poll<O> {
        match result {
            Ok(output) => {
                self.taken = true;
                Poll::Ready(output)
            }
            Err(error) => {
                self.error = Some(error.clone());
                self.parked = true;

                if self.group.is_some() {
                    Poll::Pending
                } else {
                    fatal(error)
                }
            }
        }
    }

    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::Timeout<&mut Self> {
        crate::Timeout::browser(self, duration, |_| crate::timeout::WaitStatus {
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
    type Output = O;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if this.parked {
            return Poll::Pending;
        }

        if this.taken {
            crate::message::consumed();
        }

        match Pin::new(&mut this.response).poll(context) {
            Poll::Ready(Ok(result)) => this.ready(result),
            Poll::Ready(Err(_)) => this.missing(),
            Poll::Pending => Poll::Pending,
        }
    }
}
