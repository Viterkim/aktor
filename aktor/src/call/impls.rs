use super::*;

impl<T, I, R, L> Call<T, I, R, L> {
    #[doc(hidden)]
    pub fn new(
        target: T,
        input: I,
        operation: Operation,
        request: fn(T, I, Operation) -> R,
        factory: L,
    ) -> Self {
        Self {
            target,
            input,
            request,
            factory,
            operation,
        }
    }

    /// Send this input, then keep updating the session.
    // The session types are named as generics, not `L::Sender`, so an unmarked function gets
    // `Factory`'s message instead of "method exists but its trait bounds were not satisfied".
    pub fn latest<Sender, Results>(self) -> (Sender, Results)
    where
        L: Factory<T, I, Sender = Sender, Results = Results>,
        Sender: crate::latest::SendLatest<I>,
    {
        let (sender, results) = self.factory.create(self.target, self.operation);
        crate::latest::SendLatest::send(&sender, self.input);
        (sender, results)
    }

    pub(super) fn into_request(self) -> R {
        (self.request)(self.target, self.input, self.operation)
    }

    /// Wait for queue admission and keep the reply for later.
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

    pub fn timeout(self, duration: core::time::Duration) -> R::Timed
    where
        R: TimeoutRequest,
    {
        self.into_request().timeout(duration)
    }

    pub fn retry(self, options: R::Options) -> Retried<T, I, R, L>
    where
        R: RetryRequest,
    {
        Retried::new(self, options)
    }
}
impl<T, I, R: Future, L> IntoFuture for Call<T, I, R, L> {
    type Output = R::Output;
    type IntoFuture = R;

    fn into_future(self) -> R {
        self.into_request()
    }
}
