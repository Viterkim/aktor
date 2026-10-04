use super::*;
use crate::{
    Timeout,
    worker::{TrySendError, WorkerReply, WorkerRequest},
};
use core::time::Duration;
use serde::de::DeserializeOwned;

impl<'a, T, I, S, O: DeserializeOwned, Role, L> Call<T, I, WorkerRequest<'a, S, O, Role>, L> {
    pub async fn send(self) -> WorkerReply<O> {
        self.into_request().send().await
    }

    #[allow(clippy::result_large_err)]
    pub fn try_send(self) -> Result<WorkerReply<O>, TrySendError<WorkerRequest<'a, S, O, Role>>> {
        self.into_request().try_send()
    }

    pub fn timeout(self, duration: Duration) -> Timeout<WorkerRequest<'a, S, O, Role>> {
        self.into_request().timeout(duration)
    }
}
impl<'a, T, I, S, Role, L> Call<T, I, WorkerRequest<'a, S, (), Role>, L> {
    pub async fn cast(self) {
        self.into_request().cast().await;
    }

    #[allow(clippy::result_large_err)]
    pub fn try_cast(self) -> Result<(), TrySendError<WorkerRequest<'a, S, (), Role>>> {
        self.into_request().try_cast()
    }
}
