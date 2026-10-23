use super::*;
use crate::{
    Timeout,
    dispatch::Queued,
    message::{Reply, Request},
};
use core::time::Duration;

impl<'a, T, I, S: 'static, O, L> Call<T, I, Request<'a, S, O>, L> {
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
    type Timed = crate::Timeout<Self>;

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
    type Timed = Timeout<Request<'a, S, O>>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
