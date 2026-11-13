use super::*;
use crate::{
    AktorTimeout,
    dispatch::Queued,
    message::{CallError, Reply, Request},
};
use core::time::Duration;

impl<'a, T, I, S: 'static, O, L> Call<T, I, Request<'a, S, O>, L> {
    /// Wait for the call to enter the queue and return a reply to await later. If
    /// admission fails, return the error. Use `receive_settled()` on the reply to
    /// get the outcome during cleanup.
    pub async fn admit(self) -> Result<Reply<O>, CallError> {
        self.into_request().admit().await
    }

    /// If the queue has closed, return `NotAdmitted`. Once admitted, wait for the
    /// call's outcome through shutdown. Use [`admit`](Self::admit) to keep the reply
    /// and [`receive_settled`](Reply::receive_settled) later.
    pub async fn settle(self) -> Result<O, CallError> {
        self.admit().await?.receive_settled().await
    }

    pub fn operation(mut self, operation: Operation) -> Self {
        self.operation = operation;

        if let Some(request) = self.running.take() {
            self.running = Some(request.operation(operation));
        }

        self
    }
}
impl<'a, T, I, S: 'static, O: Send + 'static, Role, L> Call<T, I, Queued<'a, S, I, O, Role>, L>
where
    I: Send + 'static,
{
    /// Wait for the call to enter the queue and return a reply to await later. If
    /// admission fails, return the error. Use `receive_settled()` on the reply to
    /// get the outcome during cleanup.
    pub async fn admit(self) -> Result<Reply<O>, CallError> {
        self.into_request().into_request().admit().await
    }

    /// If the queue has closed, return `NotAdmitted`. Once admitted, wait for the
    /// call's outcome through shutdown. Use [`admit`](Self::admit) to keep the reply
    /// and [`receive_settled`](Reply::receive_settled) later.
    pub async fn settle(self) -> Result<O, CallError> {
        self.admit().await?.receive_settled().await
    }

    pub fn operation(mut self, operation: Operation) -> Self {
        self.operation = operation;

        if let Some(request) = self.running.take() {
            self.running = Some(request.operation(operation));
        }

        self
    }
}

impl<'a, S: 'static, O> SendRequest for Request<'a, S, O> {
    type Reply = Reply<O>;

    async fn send(self) -> Self::Reply {
        self.send().await
    }
}
impl<'a, S: 'static, O> TimeoutRequest for Request<'a, S, O> {
    type Timed = crate::AktorTimeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S: 'static> CastRequest for Request<'a, S, ()> {
    type Output = ();

    async fn cast(self) {
        self.cast().await;
    }
}

impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> SendRequest
    for Queued<'a, S, I, O, Role>
{
    type Reply = Reply<O>;

    async fn send(self) -> Self::Reply {
        self.send().await
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
