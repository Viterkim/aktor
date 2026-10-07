use super::*;
use crate::{
    Timeout,
    worker::{WorkerReply, WorkerRequest},
};
use core::time::Duration;

impl<'a, T, I, S, O, Role, L> Call<T, I, WorkerRequest<'a, S, O, Role>, L> {
    pub async fn send(self) -> WorkerReply<O> {
        self.into_request().send().await
    }

    pub fn timeout(self, duration: Duration) -> Timeout<WorkerRequest<'a, S, O, Role>> {
        self.into_request().timeout(duration)
    }
}
impl<'a, T, I, S, Role, L> Call<T, I, WorkerRequest<'a, S, (), Role>, L> {
    pub async fn cast(self) {
        self.into_request().cast().await;
    }
}
