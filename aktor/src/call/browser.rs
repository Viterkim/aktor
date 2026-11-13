use super::*;
use crate::worker::{WorkerError, WorkerReply, WorkerRequest};
use core::time::Duration;

impl<'a, T, I, S, O: 'static, Role, L> Call<T, I, WorkerRequest<'a, S, O, Role>, L> {
    /// Wait for the call to enter the queue and return a reply to await later. If
    /// admission fails, return the error. Use `receive_settled()` on the reply to
    /// get the outcome during cleanup.
    pub async fn admit(self) -> Result<WorkerReply<O>, (WorkerError, String)> {
        self.into_request().admit().await
    }

    /// If the queue has closed, return `NotAdmitted`. Once admitted, wait for the
    /// call's outcome through shutdown. Use [`admit`](Self::admit) to keep the reply
    /// and [`receive_settled`](WorkerReply::receive_settled) later.
    pub async fn settle(self) -> Result<O, (WorkerError, String)> {
        self.admit().await?.receive_settled().await
    }
}

impl<'a, S, O: 'static, Role> SendRequest for WorkerRequest<'a, S, O, Role> {
    type Reply = WorkerReply<O>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<'a, S, O: 'static, Role> TimeoutRequest for WorkerRequest<'a, S, O, Role> {
    type Timed = crate::AktorTimeout<Self>;

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
