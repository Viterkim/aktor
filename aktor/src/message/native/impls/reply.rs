use super::super::*;
use core::task::ready;

impl<O> Reply<O> {
    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::Timeout<&mut Self> {
        crate::Timeout::native(self, duration, |reply| crate::timeout::WaitStatus {
            admitted: true,
            stopping: reply
                .group
                .as_ref()
                .is_some_and(|group| group.is_stopping()),
        })
    }

    pub async fn wait_closed(&mut self) {
        let _result = self.finished.changed().await;
    }

    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<Result<O, CallError>> {
        if self.closing.is_none() {
            match ready!(self.answer.poll(cx)) {
                Ok(output) => return Poll::Ready(Ok(output)),
                Err(error) => {
                    let mut finished = self.finished.clone();
                    self.closing = Some(Box::pin(async move {
                        let _result = finished.changed().await;
                    }));
                    self.error = Some(error);
                }
            }
        }

        if let Some(closing) = &mut self.closing {
            ready!(closing.as_mut().poll(cx));
        }

        match self.error.take() {
            Some(error) => Poll::Ready(Err(error)),
            None => Poll::Ready(Err(CallError::OutcomeUnknown)),
        }
    }
}
impl<O> Future for Reply<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();
        if this.parked {
            return Poll::Pending;
        }
        match ready!(this.poll_result(cx)) {
            Ok(output) => Poll::Ready(output),
            Err(_) if this.group.is_some() => {
                this.admission.lost();
                this.parked = true;
                Poll::Pending
            }
            Err(_) => stopped(),
        }
    }
}
impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        self.answer.abandon();
    }
}
