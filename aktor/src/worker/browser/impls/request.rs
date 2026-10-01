use super::super::*;
use core::{
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};

impl<'a, S, O: DeserializeOwned, Role> WorkerRequest<'a, S, O, Role> {
    pub fn new(
        worker: &'a Worker<S, Role>,
        operation: &str,
        input: Result<String, WorkerError>,
    ) -> Self {
        Self {
            worker,
            operation: operation.into(),
            input: Some(input),
            admission: None,
            reply: None,
            latest: None,
        }
    }

    pub fn latest(mut self, key: impl Into<String>) -> Self {
        self.latest = Some(key.into());
        self.admission = None;
        self
    }

    fn replace(&mut self) -> Result<bool, WorkerError> {
        let size = self.size()?;

        let Some(key) = &self.latest else {
            return Ok(false);
        };

        let inner = &self.worker.inner;
        if inner.closed.get() {
            return Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Closed,
            ));
        }

        if inner.next.get() == u64::MAX {
            return Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Closed,
            ));
        }

        let pair = (self.operation.clone(), key.clone());
        let old_id = {
            let outstanding = inner.outstanding.borrow();
            inner.queue.borrow().iter().copied().find(|id| {
                outstanding
                    .get(id)
                    .is_some_and(|work| work.latest.as_ref() == Some(&pair))
            })
        };

        let Some(old_id) = old_id else {
            return Ok(false);
        };

        let old_bytes = inner
            .outstanding
            .borrow()
            .get(&old_id)
            .map(|work| work._bytes.num_permits())
            .unwrap_or(0);
        let extra = size.saturating_sub(old_bytes);
        let extra = match inner.bytes.clone().try_acquire_many_owned(extra as u32) {
            Ok(permit) => permit,
            Err(_) => return Ok(false),
        };

        let Some(mut old) = inner.outstanding.borrow_mut().remove(&old_id) else {
            return Ok(false);
        };

        inner.queue.borrow_mut().retain(|id| *id != old_id);
        old._bytes.merge(extra);
        if old_bytes > size {
            drop(old._bytes.split(old_bytes - size));
        }

        self.submit(old._count, old._bytes)?;
        if let Some(answer) = old.answer {
            let _sent = answer.send(Err(WorkerError::new(
                CallError::Superseded,
                WorkerCause::Superseded,
            )));
        }

        Ok(true)
    }

    fn size(&self) -> Result<usize, WorkerError> {
        let input = self
            .input
            .as_ref()
            .ok_or_else(|| WorkerError::new(CallError::NotAdmitted, WorkerCause::Closed))?
            .as_ref()
            .map_err(Clone::clone)?;
        let bytes = input
            .len()
            .checked_add(
                self.operation
                    .len()
                    .saturating_add(self.latest.as_ref().map_or(0, String::len)),
            )
            .ok_or_else(|| {
                WorkerError::new(CallError::NotAdmitted, WorkerCause::PayloadTooLarge)
            })?;

        if bytes > self.worker.inner.options.max_payload_bytes {
            return Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::PayloadTooLarge,
            ));
        }

        Ok(bytes)
    }

    fn submit(
        &mut self,
        count: OwnedSemaphorePermit,
        bytes: OwnedSemaphorePermit,
    ) -> Result<(), WorkerError> {
        let inner = &self.worker.inner;
        if inner.closed.get() {
            return Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Closed,
            ));
        }

        let id = inner.next.get();
        inner.next.set(
            id.checked_add(1)
                .ok_or_else(|| WorkerError::new(CallError::NotAdmitted, WorkerCause::Closed))?,
        );

        let input = self
            .input
            .take()
            .ok_or_else(|| WorkerError::new(CallError::NotAdmitted, WorkerCause::Closed))??;
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
                latest: self
                    .latest
                    .as_ref()
                    .map(|key| (self.operation.clone(), key.clone())),
                _count: count,
                _bytes: bytes,
            },
        );
        inner.queue.borrow_mut().push_back(id);
        inner.pump();

        self.reply = Some(WorkerReply {
            response,
            inner: Rc::downgrade(inner),
            id,
            timeout: TimeoutFuture::new(inner.options.timeout_ms),
            output: PhantomData,
        });

        Ok(())
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WorkerError>> {
        if self.reply.is_some() {
            return Poll::Ready(Ok(()));
        }

        if self.replace()? {
            self.admission = None;
            return Poll::Ready(Ok(()));
        }

        if self.admission.is_none() {
            let size = self.size()?;
            let inner = &self.worker.inner;
            self.admission = Some(Box::pin(async move {
                observe(inner.ready.subscribe()).await?;

                let closed = || WorkerError::new(CallError::NotAdmitted, WorkerCause::Closed);
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
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    pub async fn checked(mut self) -> Result<O, WorkerError> {
        poll_fn(|cx| self.poll_submit(cx)).await?;

        self.reply
            .take()
            .ok_or_else(|| WorkerError::new(CallError::NotAdmitted, WorkerCause::Closed))?
            .checked()
            .await
    }

    pub async fn send(self) -> WorkerReply<O> {
        match self.checked_send().await {
            Ok(reply) => reply.0,
            Err(error) => fatal(error),
        }
    }

    pub async fn checked_send(mut self) -> Result<CheckedWorkerReply<O>, WorkerError> {
        poll_fn(|cx| self.poll_submit(cx)).await?;

        self.reply
            .take()
            .map(CheckedWorkerReply)
            .ok_or_else(|| WorkerError::new(CallError::NotAdmitted, WorkerCause::Closed))
    }

    #[allow(clippy::result_large_err)]
    pub fn try_send(mut self) -> Result<WorkerReply<O>, TrySendError<Self>> {
        self.admission = None;

        if let Some(reply) = self.reply.take() {
            return Ok(reply);
        }

        let inner = &self.worker.inner;
        if inner.closed.get() || matches!(*inner.ready.borrow(), Some(Err(_))) {
            return Err(TrySendError::Closed(self));
        }

        if inner.ready.borrow().is_none() {
            return Err(TrySendError::Full(self));
        }

        match self.replace() {
            Ok(true) => return self.reply.take().ok_or(TrySendError::Closed(self)),
            Ok(false) => {}
            Err(error) => return Err(TrySendError::Rejected(self, error)),
        }

        let size = match self.size() {
            Ok(size) => size,
            Err(error) => return Err(TrySendError::Rejected(self, error)),
        };

        let Ok(count) = inner.count.clone().try_acquire_owned() else {
            return Err(TrySendError::Full(self));
        };

        let Ok(bytes) = inner.bytes.clone().try_acquire_many_owned(size as u32) else {
            return Err(TrySendError::Full(self));
        };

        match self.submit(count, bytes) {
            Ok(()) => match self.reply.take() {
                Some(reply) => Ok(reply),
                None => Err(TrySendError::Closed(self)),
            },
            Err(error) => {
                self.input = Some(Err(error.clone()));
                if error.cause == WorkerCause::Closed {
                    Err(TrySendError::Closed(self))
                } else {
                    Err(TrySendError::Rejected(self, error))
                }
            }
        }
    }
}
impl<S, O: DeserializeOwned, Role> Future for WorkerRequest<'_, S, O, Role> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        match this.poll_submit(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => fatal(error),
            Poll::Pending => return Poll::Pending,
        }

        match this.reply.as_mut().map(|reply| reply.poll_checked(cx)) {
            Some(Poll::Ready(Ok(output))) => Poll::Ready(output),
            Some(Poll::Ready(Err(error))) => fatal(error),
            _ => Poll::Pending,
        }
    }
}

impl<S, Role> WorkerRequest<'_, S, (), Role> {
    pub async fn cast(self) {
        drop(self.send().await);
    }

    pub async fn checked_cast(self) -> Result<(), WorkerError> {
        drop(self.checked_send().await?);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub fn try_cast(self) -> Result<(), TrySendError<Self>> {
        self.try_send().map(drop)
    }
}
