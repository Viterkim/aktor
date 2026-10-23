use super::*;
use crate::local::{Reply, Request};
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
use core::time::Duration;

impl<'a, S: 'static, const N: usize, E, O: 'static, Role, Clock> SendRequest
    for Request<'a, S, N, E, O, Role, Clock>
{
    type Reply = Reply<O, Clock>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
impl<'a, S: 'static, const N: usize, E, O: 'static, Role, Clock> TimeoutRequest
    for Request<'a, S, N, E, O, Role, Clock>
where
    Clock: crate::local::clock::AktorClock,
{
    type Timed = crate::Timeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S: 'static, const N: usize, E, Role, Clock> CastRequest
    for Request<'a, S, N, E, (), Role, Clock>
{
    type Output = ();

    async fn cast(self) {
        self.cast().await;
    }
}
