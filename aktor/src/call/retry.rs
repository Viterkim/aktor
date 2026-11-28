use super::*;
use core::time::Duration;

#[must_use = "await the call, send it, or make a latest session"]
pub struct Retried<T, I, R: RetryRequest, L> {
    call: Call<T, I, R, L>,
    options: R::Options,
}

impl<T, I, R: RetryRequest, L> Retried<T, I, R, L> {
    pub(super) fn new(call: Call<T, I, R, L>, options: R::Options) -> Self {
        Self { call, options }
    }

    fn into_request(self) -> R {
        self.call.into_request().retry(self.options)
    }

    pub fn latest<Sender, Results>(self) -> (Sender, Results)
    where
        L: Factory<T, I, Sender = Sender, Results = Results>,
        Sender: RetryRequest<Options = R::Options> + crate::latest::SendLatest<I>,
    {
        let call = self.call;
        let (sender, results) = call.factory.create(call.target, call.operation);
        let sender = sender.retry(self.options);

        crate::latest::SendLatest::send(&sender, call.input);
        (sender, results)
    }

    pub fn send(self) -> R::Send
    where
        R: SendRequest,
    {
        self.into_request().send()
    }

    pub fn cast(self) -> R::Cast
    where
        R: CastRequest,
    {
        self.into_request().cast()
    }

    pub fn timeout(self, duration: Duration) -> R::Timed
    where
        R: TimeoutRequest,
    {
        self.into_request().timeout(duration)
    }
}

impl<T, I, R: RetryRequest + Future, L> IntoFuture for Retried<T, I, R, L> {
    type Output = R::Output;
    type IntoFuture = R;

    fn into_future(self) -> R {
        self.into_request()
    }
}
