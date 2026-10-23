use super::*;
use crate::task::{AktorTaskReply, AktorTaskRequest, AktorTaskState};
use core::time::Duration;

impl<'a, S, I, O, Fut, Role> SendRequest for AktorTaskRequest<'a, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Reply = AktorTaskReply<O>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<'a, S, I, O, Fut, Role> TimeoutRequest for AktorTaskRequest<'a, S, I, O, Fut, Role>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    type Timed = crate::Timeout<Self>;

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
    type Output = ();

    async fn cast(self) {
        drop(self.send().await);
    }
}
