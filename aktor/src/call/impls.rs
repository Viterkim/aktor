use super::*;
use core::task::{Context, Poll};

impl<T, I, R, L> Call<T, I, R, L> {
    #[doc(hidden)]
    pub fn new(
        target: T,
        input: I,
        operation: Operation,
        request: fn(T, I, Operation) -> R,
        latest: L,
    ) -> Self {
        Self {
            pending: Some((target, input)),
            running: None,
            request,
            latest,
            operation,
        }
    }

    /// Send this input, then keep updating the session.
    pub fn latest(self) -> (L::Sender, L::Results)
    where
        L: Factory<T, I>,
    {
        let Some((target, input)) = self.pending else {
            crate::message::consumed();
        };

        let (sender, results) = self.latest.create(target, self.operation);
        crate::latest::SendLatest::send(&sender, input);
        (sender, results)
    }

    pub fn into_request(mut self) -> R
    where
        R: Unpin,
    {
        if let Some(request) = self.running.take() {
            return request;
        }

        let Some((target, input)) = self.pending.take() else {
            crate::message::consumed();
        };

        (self.request)(target, input, self.operation)
    }

    /// Wait for queue admission and keep the reply for later.
    pub async fn send(self) -> R::Reply
    where
        R: SendRequest + Unpin,
    {
        self.into_request().send().await
    }

    pub async fn cast(self) -> R::Output
    where
        R: CastRequest + Unpin,
    {
        self.into_request().cast().await
    }

    pub fn timeout(self, duration: core::time::Duration) -> R::Timed
    where
        R: TimeoutRequest + Unpin,
    {
        self.into_request().timeout(duration)
    }

    pub fn retry(self, options: R::Options) -> Retried<T, I, R, L>
    where
        R: RetryRequest + Unpin,
    {
        Retried::new(self, options)
    }
}
impl<T, I, R: Future, L> Future for Call<T, I, R, L> {
    type Output = R::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if this.running.as_ref().get_ref().is_none() {
            let Some((target, input)) = this.pending.take() else {
                crate::message::consumed();
            };

            let request = (this.request)(target, input, *this.operation);

            this.running.set(Some(request));
        }

        match this.running.as_pin_mut() {
            Some(request) => request.poll(cx),
            None => crate::message::consumed(),
        }
    }
}
