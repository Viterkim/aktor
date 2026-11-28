use super::*;
use crate::{
    AktorTimeout,
    dispatch::Queued,
    message::{Reply, Request, RequestCast, RequestSend},
};
use core::time::Duration;

impl<'a, T, I, S: 'static, O, L> Call<T, I, Request<'a, S, O>, L> {
    pub fn operation(mut self, operation: Operation) -> Self {
        self.operation = operation;
        self
    }
}
impl<'a, T, I, S: 'static, O: Send + 'static, Role, L> Call<T, I, Queued<'a, S, I, O, Role>, L>
where
    I: Send + 'static,
{
    pub fn operation(mut self, operation: Operation) -> Self {
        self.operation = operation;
        self
    }
}

impl<'a, S: 'static, O> SendRequest for Request<'a, S, O> {
    type Reply = Reply<O>;
    type Send = RequestSend<'a, S, O>;

    fn send(self) -> Self::Send {
        self.send()
    }
}
impl<'a, S: 'static, O> TimeoutRequest for Request<'a, S, O> {
    type Timed = AktorTimeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S: 'static> CastRequest for Request<'a, S, ()> {
    type Output = Result<(), crate::AktorDeliveryError>;
    type Cast = RequestCast<'a, S>;

    fn cast(self) -> Self::Cast {
        self.cast()
    }
}

impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> SendRequest
    for Queued<'a, S, I, O, Role>
{
    type Reply = Reply<O>;
    type Send = RequestSend<'a, S, O>;

    fn send(self) -> Self::Send {
        self.send()
    }
}
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> TimeoutRequest
    for Queued<'a, S, I, O, Role>
{
    type Timed = AktorTimeout<Request<'a, S, O>>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
