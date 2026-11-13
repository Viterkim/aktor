use core::{future::Future, time::Duration};

#[doc(hidden)]
pub trait SendRequest {
    type Reply;

    fn send(self) -> impl Future<Output = Self::Reply>;
}

#[doc(hidden)]
pub trait CastRequest {
    type Output;

    fn cast(self) -> impl Future<Output = Self::Output>;
}

#[doc(hidden)]
pub trait TimeoutRequest {
    type Timed;

    fn timeout(self, duration: Duration) -> Self::Timed;
}

#[doc(hidden)]
pub trait RetryRequest: Sized {
    type Options;

    fn retry(self, options: Self::Options) -> Self;
}
