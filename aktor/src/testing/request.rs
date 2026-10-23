use super::Handlers;
use crate::{
    call::{CastRequest, SendRequest, TimeoutRequest},
    listener::CompletionObserver,
    message,
    timeout::WaitStatus,
};
use alloc::boxed::Box;
use core::{
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use futures_util::Stream;

#[cfg(all(test, feature = "tokio"))]
mod tests;

type Admission<'a, O> =
    Pin<Box<dyn Future<Output = Result<message::Reply<O>, message::CallError>> + Send + 'a>>;
type Answer<O> = Pin<Box<dyn Future<Output = O> + Send>>;

pub struct Request<'a, O> {
    admission: Option<Admission<'a, O>>,
    reply: Option<Reply<O>>,
    standard: bool,
}

pub struct Reply<O> {
    answer: Answer<O>,
    standard: bool,
}

pub struct LatestResults<O> {
    results: message::LatestResults<O>,
    closed: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<'a, O: Send + 'static> Request<'a, O> {
    pub fn new(request: message::Request<'a, Handlers, O>, standard: bool) -> Self {
        Self {
            admission: Some(Box::pin(request.admit())),
            reply: None,
            standard,
        }
    }
}
impl<O: Send + 'static> Future for Request<'_, O> {
    type Output = O;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        if self.reply.is_none() {
            let Some(admission) = &mut self.admission else {
                panic!("test request already consumed");
            };
            let reply = core::task::ready!(admission.as_mut().poll(cx));
            self.admission = None;
            self.reply = Some(Reply::new(reply, self.standard));
        }
        match &mut self.reply {
            Some(reply) => Pin::new(reply).poll(cx),
            None => panic!("test request has no reply"),
        }
    }
}
impl<O: Send + 'static> Reply<O> {
    fn new(reply: Result<message::Reply<O>, message::CallError>, standard: bool) -> Self {
        let reply = reply.unwrap_or_else(|error| panic!("test actor refused the call: {error:?}"));
        Self {
            answer: Box::pin(async move {
                reply.receive().await.unwrap_or_else(|error| {
                    panic!("test actor failed before returning its result: {error:?}")
                })
            }),
            standard,
        }
    }
}
impl<O> Future for Reply<O> {
    type Output = O;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        self.answer.as_mut().poll(cx)
    }
}
impl<O: Send + 'static> SendRequest for Request<'_, O> {
    type Reply = Reply<O>;

    async fn send(mut self) -> Self::Reply {
        if let Some(reply) = self.reply.take() {
            return reply;
        }
        let Some(admission) = self.admission.take() else {
            panic!("test request already consumed");
        };
        Reply::new(admission.await, self.standard)
    }
}
impl CastRequest for Request<'_, ()> {
    type Output = ();

    async fn cast(self) {
        drop(self.send().await);
    }
}
impl<O> TimeoutRequest for Request<'_, O> {
    type Timed = crate::Timeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        let standard = self.standard;
        crate::Timeout::queued(
            self,
            duration,
            |request| WaitStatus {
                admitted: request.reply.is_some(),
            },
            standard,
        )
    }
}

impl<O> Reply<O> {
    pub fn timeout(&mut self, duration: Duration) -> crate::Timeout<&mut Self> {
        let standard = self.standard;
        crate::Timeout::queued(self, duration, |_| WaitStatus { admitted: true }, standard)
    }
}

impl<O> LatestResults<O> {
    pub fn new(results: message::LatestResults<O>, mut completion: CompletionObserver) -> Self {
        Self {
            results,
            closed: Some(Box::pin(async move { completion.wait().await })),
        }
    }

    pub async fn next(&mut self) -> Option<O> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }
}
impl<O> futures_util::Stream for LatestResults<O> {
    type Item = O;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<O>> {
        if let Poll::Ready(output) = Pin::new(&mut self.results).poll_next(cx) {
            return Poll::Ready(output);
        }
        if let Some(closed) = &mut self.closed
            && closed.as_mut().poll(cx).is_ready()
        {
            self.closed = None;
        }
        if self.closed.is_none() {
            return match Pin::new(&mut self.results).poll_next(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => panic!("test actor failed before returning its latest result"),
            };
        }
        Poll::Pending
    }
}
