use super::*;
use crate::{
    local::{Reply, Request},
    message::TrySendError,
};
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
use core::time::Duration;

impl<'a, T, I, S: 'static, const N: usize, E, O: 'static, Role, Clock, L>
    Call<T, I, Request<'a, S, N, E, O, Role, Clock>, L>
{
    pub async fn send(self) -> Reply<O, Clock> {
        self.into_request().send().await
    }

    #[allow(clippy::type_complexity)]
    pub fn try_send(
        self,
    ) -> Result<Reply<O, Clock>, TrySendError<Request<'a, S, N, E, O, Role, Clock>>> {
        self.into_request().try_send()
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "embassy",
        feature = "browser_local"
    ))]
    pub fn timeout(self, duration: Duration) -> crate::Timeout<Request<'a, S, N, E, O, Role, Clock>>
    where
        Clock: crate::local::clock::AktorClock,
    {
        self.into_request().timeout(duration)
    }
}
impl<'a, T, I, S: 'static, const N: usize, E, Role, Clock, L>
    Call<T, I, Request<'a, S, N, E, (), Role, Clock>, L>
{
    pub async fn cast(self) {
        self.into_request().cast().await;
    }

    #[allow(clippy::type_complexity)]
    pub fn try_cast(self) -> Result<(), TrySendError<Request<'a, S, N, E, (), Role, Clock>>> {
        self.into_request().try_cast()
    }
}
