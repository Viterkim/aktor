use core::{future::Future, time::Duration};

pub trait SendRequest {
    type Reply;

    fn send(self) -> impl Future<Output = Self::Reply>;
}

pub trait CastRequest {
    type Output;

    fn cast(self) -> impl Future<Output = Self::Output>;
}

pub trait TimeoutRequest {
    type Timed;

    fn timeout(self, duration: Duration) -> Self::Timed;
}

pub trait RetryRequest: Sized {
    type Options;

    fn retry(self, options: Self::Options) -> Self;
}
