use super::*;
use crate::{
    Timeout,
    message::TrySendError,
    task::{AktorTaskReply, AktorTaskRequest, AktorTaskState},
};
use core::time::Duration;

impl<'a, T, I, S, O, Fut, Role, L> Call<T, I, AktorTaskRequest<'a, S, I, O, Fut, Role>, L>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    pub async fn send(self) -> AktorTaskReply<O> {
        self.into_request().send().await
    }

    #[allow(clippy::type_complexity)]
    pub fn try_send(
        self,
    ) -> Result<AktorTaskReply<O>, TrySendError<AktorTaskRequest<'a, S, I, O, Fut, Role>>> {
        self.into_request().try_send()
    }

    pub fn timeout(self, duration: Duration) -> Timeout<AktorTaskRequest<'a, S, I, O, Fut, Role>> {
        self.into_request().timeout(duration)
    }
}
impl<'a, T, I, S, Fut, Role, L> Call<T, I, AktorTaskRequest<'a, S, I, (), Fut, Role>, L>
where
    S: Send + 'static,
    I: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, ())> + Send + 'static,
{
    pub async fn cast(self) {
        drop(self.send().await);
    }

    #[allow(clippy::type_complexity)]
    pub fn try_cast(self) -> Result<(), TrySendError<AktorTaskRequest<'a, S, I, (), Fut, Role>>> {
        self.try_send().map(drop)
    }
}
