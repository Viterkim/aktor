use super::super::*;
use core::task::ready;

impl<O> Reply<O> {
    pub fn checked(self) -> CheckedReply<O> {
        CheckedReply(self)
    }

    pub async fn wait_closed(&mut self) {
        let _result = self.finished.changed().await;
    }

    pub fn poll_checked_result(&mut self, cx: &mut Context<'_>) -> Poll<Result<O, CallError>> {
        if let Some(error) = self.error.take() {
            return Poll::Ready(Err(error));
        }

        self.answer.poll(cx)
    }

    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<Result<O, CallError>> {
        if self.closing.is_none() {
            match ready!(self.answer.poll(cx)) {
                Ok(output) => return Poll::Ready(Ok(output)),
                Err(CallError::Superseded) => return Poll::Ready(Err(CallError::Superseded)),
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
        match ready!(self.get_mut().poll_result(cx)) {
            Ok(output) => Poll::Ready(output),
            Err(CallError::Superseded) => panic!("actor call superseded before execution"),
            Err(_) => stopped(),
        }
    }
}
impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        self.answer.abandon();
    }
}

impl<O> Future for CheckedReply<O> {
    type Output = Result<O, CallError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_checked_result(cx)
    }
}
