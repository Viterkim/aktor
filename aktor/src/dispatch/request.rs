use super::*;
use crate::{
    Timeout,
    message::{Reply, TrySendError},
};
use core::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

trait Body<S, I, O> {
    fn run(self: Box<Self>, state: &mut S, input: I) -> LocalFuture<'_, O>;
}

struct ReadBody<F>(F);
impl<S: 'static, I: 'static, O: 'static, F> Body<S, I, O> for ReadBody<F>
where
    F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
{
    fn run(self: Box<Self>, state: &mut S, input: I) -> LocalFuture<'_, O> {
        Box::pin(async move { self.0(state, input).await })
    }
}

struct WriteBody<F>(F);
impl<S: 'static, I: 'static, O: 'static, F> Body<S, I, O> for WriteBody<F>
where
    F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
{
    fn run(self: Box<Self>, state: &mut S, input: I) -> LocalFuture<'_, O> {
        Box::pin(async move { self.0(state, input).await })
    }
}

type Pending<S, I, O> = Box<(I, Box<dyn Body<S, I, O> + Send>)>;

#[doc(hidden)]
pub struct Queued<'a, S, I, O, Role = ()> {
    handle: &'a Handle<S, Role>,
    pending: Option<Pending<S, I, O>>,
    operation: Operation,
    running: Option<Request<'a, S, O>>,
}
impl<'a, S: 'static, I: 'static, O: 'static, Role> Queued<'a, S, I, O, Role> {
    pub fn operation(mut self, operation: Operation) -> Self {
        self.operation = operation;

        if let Some(request) = self.running.take() {
            self.running = Some(request.operation(operation));
        }

        self
    }

    pub fn read<F>(handle: &'a Handle<S, Role>, operation: Operation, function: F, input: I) -> Self
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
    {
        Self {
            handle,
            pending: Some(Box::new((input, Box::new(ReadBody(function))))),
            operation,
            running: None,
        }
    }

    pub fn write<F>(
        handle: &'a Handle<S, Role>,
        operation: Operation,
        function: F,
        input: I,
    ) -> Self
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
    {
        Self {
            handle,
            pending: Some(Box::new((input, Box::new(WriteBody(function))))),
            operation,
            running: None,
        }
    }
}
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> Queued<'a, S, I, O, Role> {
    pub fn into_request(mut self) -> Request<'a, S, O> {
        if let Some(request) = self.running.take() {
            return request;
        }

        let Some(pending) = self.pending.take() else {
            crate::message::consumed();
        };
        let (input, body) = *pending;

        call_async(
            self.handle,
            async move |state: &mut S, input| body.run(state, input).await,
            input,
        )
        .operation(self.operation)
    }

    pub async fn send(self) -> Reply<O> {
        self.into_request().send().await
    }

    #[allow(clippy::result_large_err)]
    pub fn try_send(self) -> Result<Reply<O>, TrySendError<Request<'a, S, O>>> {
        self.into_request().try_send()
    }

    pub fn timeout(self, duration: Duration) -> Timeout<Request<'a, S, O>> {
        self.into_request().timeout(duration)
    }
}
impl<S: 'static, I: Send + 'static, O: Send + 'static, Role> Future for Queued<'_, S, I, O, Role> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if this.running.is_none() {
            let Some(pending) = this.pending.take() else {
                crate::message::consumed();
            };
            let (input, body) = *pending;

            this.running = Some(
                call_async(
                    this.handle,
                    async move |state: &mut S, input| body.run(state, input).await,
                    input,
                )
                .operation(this.operation),
            );
        }

        match &mut this.running {
            Some(request) => Pin::new(request).poll(cx),
            None => crate::message::consumed(),
        }
    }
}
