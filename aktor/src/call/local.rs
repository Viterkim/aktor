use super::*;
use crate::local::{Reply, Request, RequestCast, RequestSend};
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
use core::time::Duration;

impl<'a, S: 'static, O: 'static, Role, Clock> SendRequest for Request<'a, S, O, Role, Clock> {
    type Reply = Reply<O, Clock>;
    type Send = RequestSend<'a, S, O, Role, Clock>;

    fn send(self) -> Self::Send {
        self.send()
    }
}
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
impl<'a, S: 'static, O: 'static, Role, Clock> TimeoutRequest for Request<'a, S, O, Role, Clock>
where
    Clock: crate::local::clock::AktorClock,
{
    type Timed = crate::AktorTimeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S: 'static, Role, Clock> CastRequest for Request<'a, S, (), Role, Clock> {
    type Output = Result<(), crate::AktorDeliveryError>;
    type Cast = RequestCast<'a, S, Role, Clock>;

    fn cast(self) -> Self::Cast {
        self.cast()
    }
}
