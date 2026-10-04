use super::*;
use crate::{
    Timeout,
    embassy::{Reply, Request},
    message::TrySendError,
};
use core::time::Duration;

impl<'a, T, I, S: 'static, const N: usize, E, O: 'static, Role, L>
    Call<T, I, Request<'a, S, N, E, O, Role>, L>
{
    pub async fn send(self) -> Reply<O> {
        self.into_request().send().await
    }

    #[allow(clippy::type_complexity)]
    pub fn try_send(self) -> Result<Reply<O>, TrySendError<Request<'a, S, N, E, O, Role>>> {
        self.into_request().try_send()
    }

    pub fn timeout(self, duration: Duration) -> Timeout<Request<'a, S, N, E, O, Role>> {
        self.into_request().timeout(duration)
    }
}
impl<'a, T, I, S: 'static, const N: usize, E, Role, L>
    Call<T, I, Request<'a, S, N, E, (), Role>, L>
{
    pub async fn cast(self) {
        self.into_request().cast().await;
    }

    pub fn try_cast(self) -> Result<(), TrySendError<Request<'a, S, N, E, (), Role>>> {
        self.into_request().try_cast()
    }
}
