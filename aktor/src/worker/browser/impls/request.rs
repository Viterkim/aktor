use super::super::*;
use core::{
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll, ready},
};

impl<'a, S, O, Role> WorkerRequest<'a, S, O, Role> {
    pub fn new(
        worker: &'a Worker<S, Role, impl Sized>,
        operation: &str,
        input: Result<Vec<u8>, WorkerError>,
    ) -> Self
    where
        O: DeserializeOwned,
    {
        Self::with_decoder(worker, operation, input, codec::decode_output)
    }

    pub fn with_decoder(
        worker: &'a Worker<S, Role, impl Sized>,
        operation: &str,
        input: Result<Vec<u8>, WorkerError>,
        decoder: fn(&[u8]) -> Result<O, WorkerError>,
    ) -> Self {
        Self {
            inner: &worker.inner,
            state: PhantomData,
            operation: operation.into(),
            input: Some(input.map_err(|error| error.without_data())),
            encoder: None,
            decoder,
            admission: None,
            reply: None,
            parked: false,
        }
    }

    fn size(&mut self) -> Result<usize, WireError> {
        if let Some(encoder) = self.encoder.take() {
            self.input = Some(encoder());
        }

        let input = self
            .input
            .as_ref()
            .ok_or_else(|| WireError::new(CallError::NotAdmitted, WorkerCause::Closed))?
            .as_ref()
            .map_err(Clone::clone)?;

        let bytes = input.len().saturating_add(self.operation.len());

        Ok(bytes.min(self.inner.options.max_outstanding_bytes))
    }

    fn submit(
        &mut self,
        count: OwnedSemaphorePermit,
        bytes: OwnedSemaphorePermit,
    ) -> Result<(), WireError> {
        let inner = self.inner;

        if inner.closed.get() {
            return Err(WireError::new(CallError::NotAdmitted, WorkerCause::Closed));
        }

        let id = inner.next.get();

        inner.next.set(
            id.checked_add(1)
                .ok_or_else(|| WireError::new(CallError::NotAdmitted, WorkerCause::Closed))?,
        );

        let input = self
            .input
            .take()
            .ok_or_else(|| WireError::new(CallError::NotAdmitted, WorkerCause::Closed))??;
        let (answer, response) = oneshot::channel();

        inner.outstanding.borrow_mut().insert(
            id,
            Work {
                answer: Some(answer),
                input: Some(Incoming::Call {
                    id,
                    operation: self.operation.clone(),
                    input,
                }),
                service: None,
                _count: Some(count),
                _bytes: Some(bytes),
            },
        );

        inner.queue.borrow_mut().push_back(id);
        inner.pump();

        self.reply = Some(WorkerReply {
            response,
            inner: Rc::downgrade(inner),
            group: inner.group.borrow().clone(),
            id,
            parked: false,
            taken: false,
            output: PhantomData,
            decoder: self.decoder,
        });

        Ok(())
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WireError>> {
        if let Some(reply) = &self.reply {
            if reply.taken {
                crate::message::consumed();
            }
            return Poll::Ready(Ok(()));
        }

        if self.admission.is_none() {
            let size = self.size()?;
            let inner = self.inner;

            self.admission = Some(Box::pin(async move {
                observe(inner.ready.subscribe()).await?;

                let closed = || WireError::new(CallError::NotAdmitted, WorkerCause::Closed);
                let count = inner
                    .count
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| closed())?;

                let bytes = inner
                    .bytes
                    .clone()
                    .acquire_many_owned(size as u32)
                    .await
                    .map_err(|_| closed())?;

                Ok((count, bytes))
            }));
        }

        let Some(admission) = &mut self.admission else {
            return Poll::Pending;
        };

        match admission.as_mut().poll(cx) {
            Poll::Ready(Ok((count, bytes))) => {
                self.admission = None;
                Poll::Ready(self.submit(count, bytes))
            }
            Poll::Ready(Err(error)) => {
                self.admission = None;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.parked {
            return Poll::Pending;
        }

        match self.poll_submit(cx) {
            Poll::Ready(Err(error)) => {
                self.parked = true;
                self.admission = None;

                if error.outcome == CallError::NotAdmitted
                    && error.cause == WorkerCause::Closed
                    && self.inner.closed.get()
                    && !self.inner.failed.get()
                {
                    let stopping = self
                        .inner
                        .group
                        .borrow()
                        .as_ref()
                        .is_some_and(|(_, group)| group.is_stopping());
                    if stopping {
                        Poll::Pending
                    } else {
                        self.inner.lost(error)
                    }
                } else {
                    self.inner.lost(error)
                }
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn timeout(self, duration: core::time::Duration) -> crate::Timeout<Self> {
        crate::Timeout::browser(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.reply.is_some(),
            stopping: request
                .inner
                .group
                .borrow()
                .as_ref()
                .is_some_and(|(_, group)| group.is_stopping()),
        })
    }

    pub async fn send(mut self) -> WorkerReply<O> {
        poll_fn(|cx| self.poll_send(cx)).await;

        match self.reply.take() {
            Some(reply) => reply,
            None => {
                core::future::pending::<()>().await;
                fatal(WireError::new(CallError::NotAdmitted, WorkerCause::Closed))
            }
        }
    }
}
impl<S, O, Role> Future for WorkerRequest<'_, S, O, Role> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if this.parked {
            return Poll::Pending;
        }

        ready!(this.poll_send(cx));

        match this.reply.as_mut().map(|reply| reply.poll_result(cx)) {
            Some(Poll::Ready(Ok(output))) => Poll::Ready(output),
            Some(Poll::Ready(Err(error))) => {
                this.parked = true;
                this.inner.lost(error)
            }
            _ => Poll::Pending,
        }
    }
}
impl<S, Role> WorkerRequest<'_, S, (), Role> {
    pub async fn cast(self) {
        drop(self.send().await);
    }
}
