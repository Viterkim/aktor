use super::*;
use crate::cross_core::{Reply, Request, RequestCast, RequestSend};
use core::time::Duration;

impl<'a, S: 'static, Args: Send + 'static, O: Send + 'static, Role> SendRequest
    for Request<'a, S, Args, O, Role>
{
    type Reply = Reply<O>;
    type Send = RequestSend<'a, S, Args, O, Role>;

    fn send(self) -> Self::Send {
        self.send()
    }
}
impl<'a, S: 'static, Args: Send + 'static, O: Send + 'static, Role> TimeoutRequest
    for Request<'a, S, Args, O, Role>
{
    type Timed = crate::AktorTimeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S: 'static, Args: Send + 'static, Role> CastRequest for Request<'a, S, Args, (), Role> {
    type Output = Result<(), crate::AktorDeliveryError>;
    type Cast = RequestCast<'a, S, Args, Role>;

    fn cast(self) -> Self::Cast {
        self.cast()
    }
}
