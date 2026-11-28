use super::*;
use crate::task::{AktorTaskCast, AktorTaskReply, AktorTaskRequest, AktorTaskSend, AktorTaskState};
use core::time::Duration;

impl<'a, S, I, O, Fut, Role> SendRequest for AktorTaskRequest<'a, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Reply = AktorTaskReply<O>;
    type Send = AktorTaskSend<'a, S, I, O, Fut, Role>;

    fn send(self) -> Self::Send {
        self.send()
    }
}
impl<'a, S, I, O, Fut, Role> TimeoutRequest for AktorTaskRequest<'a, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Timed = crate::AktorTimeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S, I, Fut, Role> CastRequest for AktorTaskRequest<'a, S, I, (), Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, ())> + Send + 'static,
{
    type Output = Result<(), crate::AktorDeliveryError>;
    type Cast = AktorTaskCast<'a, S, I, Fut, Role>;

    fn cast(self) -> Self::Cast {
        self.cast()
    }
}
