use super::*;
use crate::cross_core::{Reply, Request};
use core::time::Duration;

impl<'a, S: 'static, Args: Send + 'static, O: Send + 'static, Role> SendRequest
    for Request<'a, S, Args, O, Role>
{
    type Reply = Reply<O>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<'a, S: 'static, Args: Send + 'static, O: Send + 'static, Role> TimeoutRequest
    for Request<'a, S, Args, O, Role>
{
    type Timed = crate::Timeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S: 'static, Args: Send + 'static, Role> CastRequest for Request<'a, S, Args, (), Role> {
    type Output = ();

    async fn cast(self) {
        self.cast().await;
    }
}
