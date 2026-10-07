use super::*;
use crate::{
    Timeout,
    dispatch::Queued,
    message::{Reply, Request},
};
use core::time::Duration;

impl<'a, T, I, S: 'static, O, L> Call<T, I, Request<'a, S, O>, L> {
    pub async fn send(self) -> Reply<O> {
        self.into_request().send().await
    }

    pub fn timeout(self, duration: Duration) -> Timeout<Request<'a, S, O>> {
        self.into_request().timeout(duration)
    }

    pub fn operation(mut self, operation: Operation) -> Self {
        self.operation = operation;

        if let Some(request) = self.running.take() {
            self.running = Some(request.operation(operation));
        }

        self
    }
}
impl<'a, T, I, S: 'static, L> Call<T, I, Request<'a, S, ()>, L> {
    pub async fn cast(self) {
        self.into_request().cast().await;
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

    pub async fn send(self) -> Reply<O> {
        self.into_request().send().await
    }

    pub fn timeout(self, duration: Duration) -> Timeout<Request<'a, S, O>> {
        self.into_request().timeout(duration)
    }
}
