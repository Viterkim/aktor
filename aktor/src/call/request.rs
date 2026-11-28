use core::{future::Future, time::Duration};

#[doc(hidden)]
pub trait SendRequest {
    type Reply;
    type Send: Future<Output = Result<Self::Reply, crate::AktorDeliveryError>>;

    fn send(self) -> Self::Send;
}

#[doc(hidden)]
pub trait CastRequest {
    type Output;
    type Cast: Future<Output = Self::Output>;

    fn cast(self) -> Self::Cast;
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
