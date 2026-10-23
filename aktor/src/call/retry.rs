use super::*;
use alloc::boxed::Box;
use core::{
    task::{Context, Poll},
    time::Duration,
};

#[must_use = "await the call, send it, or make a latest session"]
pub struct Retried<T, I, R: RetryRequest, L> {
    pending: Option<Box<Pending<T, I, R, L>>>,
    running: Option<R>,
}

struct Pending<T, I, R: RetryRequest, L> {
    call: Call<T, I, R, L>,
    options: R::Options,
}

impl<T, I, R: RetryRequest + Unpin, L> Retried<T, I, R, L> {
    pub fn new(call: Call<T, I, R, L>, options: R::Options) -> Self {
        Self {
            pending: Some(Box::new(Pending { call, options })),
            running: None,
        }
    }

    pub fn into_request(mut self) -> R {
        if let Some(request) = self.running.take() {
            return request;
        }
        let Some(pending) = self.pending.take() else {
            crate::message::consumed();
        };
        let Pending { call, options } = *pending;
        call.into_request().retry(options)
    }

    pub fn latest(self) -> (L::Sender, L::Results)
    where
        L: Factory<T, I>,
        L::Sender: RetryRequest<Options = R::Options>,
    {
        let Some(pending) = self.pending else {
            crate::message::consumed();
        };
        let Pending { call, options } = *pending;
        let Some((target, input)) = call.pending else {
            crate::message::consumed();
        };
        let (sender, results) = call.latest.create(target, call.operation);
        let sender = sender.retry(options);
        crate::latest::SendLatest::send(&sender, input);
        (sender, results)
    }

    pub async fn send(self) -> R::Reply
    where
        R: SendRequest,
    {
        self.into_request().send().await
    }

    pub async fn cast(self) -> R::Output
    where
        R: CastRequest,
    {
        self.into_request().cast().await
    }

    pub fn timeout(self, duration: Duration) -> R::Timed
    where
        R: TimeoutRequest,
    {
        self.into_request().timeout(duration)
    }
}

impl<T, I, R: RetryRequest + Future + Unpin, L> Future for Retried<T, I, R, L> {
    type Output = R::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.running.is_none() {
            let Some(pending) = this.pending.take() else {
                crate::message::consumed();
            };
            let Pending { call, options } = *pending;
            this.running = Some(call.into_request().retry(options));
        }
        match &mut this.running {
            Some(request) => Pin::new(request).poll(cx),
            None => crate::message::consumed(),
        }
    }
}
