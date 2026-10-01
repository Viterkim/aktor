use super::super::*;
use core::{
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};

impl<O: DeserializeOwned> WorkerReply<O> {
    pub fn poll_checked(&mut self, context: &mut Context<'_>) -> Poll<Result<O, WorkerError>> {
        match Pin::new(&mut self.response).poll(context) {
            Poll::Ready(Ok(Ok(output))) => {
                return Poll::Ready(decode(&output).map_err(|mut error| {
                    error.outcome = CallError::OutcomeUnknown;
                    error
                }));
            }
            Poll::Ready(Ok(Err(error))) => return Poll::Ready(Err(error)),
            Poll::Ready(Err(_)) => {
                return Poll::Ready(Err(WorkerError::new(
                    CallError::OutcomeUnknown,
                    WorkerCause::Closed,
                )));
            }
            Poll::Pending => {}
        }

        if Pin::new(&mut self.timeout).poll(context).is_ready() {
            self.abandon();
            return Poll::Ready(Err(WorkerError::new(
                CallError::OutcomeUnknown,
                WorkerCause::Timeout,
            )));
        }

        Poll::Pending
    }

    pub async fn checked(mut self) -> Result<O, WorkerError> {
        poll_fn(|context| self.poll_checked(context)).await
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
        match self.get_mut().poll_checked(context) {
            Poll::Ready(Ok(output)) => Poll::Ready(output),
            Poll::Ready(Err(error)) => fatal(error),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<O: DeserializeOwned> Future for CheckedWorkerReply<O> {
    type Output = Result<O, WorkerError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_checked(context)
    }
}
