use super::*;
use crate::worker::{WorkerError, WorkerReply, WorkerRequest};
use core::time::Duration;

impl<'a, T, I, S, O: 'static, Role, L> Call<T, I, WorkerRequest<'a, S, O, Role>, L> {
    /// Wait for admission and return a refusal without waiting for cleanup.
    pub async fn admit(self) -> Result<WorkerReply<O>, WorkerError> {
        self.into_request().admit().await
    }
}

impl<'a, S, O: 'static, Role> SendRequest for WorkerRequest<'a, S, O, Role> {
    type Reply = WorkerReply<O>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<'a, S, O: 'static, Role> TimeoutRequest for WorkerRequest<'a, S, O, Role> {
    type Timed = crate::Timeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S, Role> CastRequest for WorkerRequest<'a, S, (), Role> {
    type Output = ();

    async fn cast(self) {
        self.cast().await;
    }
}
