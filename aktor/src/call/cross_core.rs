use super::*;
use crate::{
    cross_core::{Reply, Request},
    message::TrySendError,
};
use core::time::Duration;

impl<'a, T, I, S: 'static, Args: Send + 'static, O: Send + 'static, Role, L>
    Call<T, I, Request<'a, S, Args, O, Role>, L>
{
    pub async fn send(self) -> Reply<O> {
        self.into_request().send().await
    }

    #[allow(clippy::type_complexity)]
    pub fn try_send(self) -> Result<Reply<O>, TrySendError<Request<'a, S, Args, O, Role>>> {
        self.into_request().try_send()
    }

    pub fn timeout(self, duration: Duration) -> crate::Timeout<Request<'a, S, Args, O, Role>> {
        self.into_request().timeout(duration)
    }
}
impl<'a, T, I, S: 'static, Args: Send + 'static, Role, L>
    Call<T, I, Request<'a, S, Args, (), Role>, L>
{
    pub async fn cast(self) {
        self.into_request().cast().await;
    }

    #[allow(clippy::type_complexity)]
    pub fn try_cast(self) -> Result<(), TrySendError<Request<'a, S, Args, (), Role>>> {
        self.into_request().try_cast()
    }
}
