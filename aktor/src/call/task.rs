use super::*;
use crate::{
    Timeout,
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
}
