use super::super::*;
use core::{
    pin::Pin,
    task::{Context, Poll, ready},
};

#[derive(Er)]
enum AdmitErr {
    Closed,
    IdExhausted,
    NoInput,
}
impl AdmitErr {
    fn worker_error(&self) -> WorkerError {
        WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Closed)
    }
}
impl<'a, S, O: 'static, Role> WorkerRequest<'a, S, O, Role> {
    fn size(&mut self) -> Result<usize, WireFailure> {
        if let Some(encoder) = self.encoder.take() {
            self.input = Some(encoder()?);
        }
        let input = self
            .input
            .as_ref()
            .er(|| WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Closed))?;

        let bytes = input.len().saturating_add(self.operation.name.len());

        Ok(bytes.min(self.inner.options.max_outstanding_bytes.get()))
    }

    fn submit(
        &mut self,
        count: OwnedSemaphorePermit,
        bytes: OwnedSemaphorePermit,
    ) -> ErResult<(), AdmitErr> {
        let inner = self.inner;
        if inner.closed.get() {
            er_bail!(AdmitErr::Closed);
        }

        let id = inner.next.get();
        inner
            .next
            .set(id.checked_add(1).er(|| AdmitErr::IdExhausted)?);

        let input = self.input.take().er(|| AdmitErr::NoInput)?;
        let (answer, response) = oneshot::channel();
        let decoder = self.decoder;
        let answer: Answer = Box::new(move |output, record| {
            if answer.is_closed() {
                return output.err().map(record);
            }

            let output = match output.and_then(|bytes| Ok(decoder(&bytes)?)) {
                Ok(output) => Ok(output),
                Err(error) => Err(record(error)),
            };
            let failure = output.as_ref().err().cloned();

            let _sent = answer.send(output);
            failure
        });

        inner.outstanding.borrow_mut().insert(
            id,
            Work {
                operation: self.operation,
                answer: Some(answer),
                input: Some(Incoming::Call {
                    id,
                    operation: self.operation.name.into(),
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
            group: inner.group.clone(),
            id,
            taken: false,
        });

        Ok(())
    }

    pub(in crate::worker::browser) fn poll_checked(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WireFailure>> {
        let Err(mut error) = ready!(self.poll_submit(cx)) else {
            return Poll::Ready(Ok(()));
        };

        if error.error.cause == WorkerCause::Closed
            && error.error.failure_id.is_none()
            && let Some(failure) = self
                .inner
                .recorded_failure(AktorDeliveryOutcome::NotAdmitted)
        {
            error = failure;
        }

        deliver(error.error.failure_id, &self.inner.group);
        Poll::Ready(Err(error))
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WireFailure>> {
        if let Some(reply) = &self.reply {
            if reply.taken {
                crate::message::consumed();
            }
            return Poll::Ready(Ok(()));
        }

        if self.admission.is_none() {
            let size = match self.size() {
                Ok(size) => size,
                Err(mut error) => {
                    let id =
                        self.inner
                            .record_occurrence(self.operation.phase(), &error.report, false);
                    error.error.failure_id = Some(id);
                    return Poll::Ready(Err(error));
                }
            };
            let inner = self.inner;

            self.admission = Some(Box::pin(async move {
                if let Err(failure) = observe(inner.ready.subscribe()).await {
                    return Err(failure.error);
                }

                let closed =
                    || WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Closed);
                let count = inner.count.clone().acquire_owned().await.er(closed)?;

                let bytes = inner
                    .bytes
                    .clone()
                    .acquire_many_owned(size as u32)
                    .await
                    .er(closed)?;

                Ok((count, bytes))
            }));
        }

        let Some(admission) = &mut self.admission else {
            return Poll::Pending;
        };

        match admission.as_mut().poll(cx) {
            Poll::Ready(Ok((count, bytes))) => {
                self.admission = None;
                self.submit(count, bytes).er_with(AdmitErr::worker_error)?;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.admission = None;
                Poll::Ready(Err(error.with_outcome(AktorDeliveryOutcome::NotAdmitted)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn timeout(self, duration: core::time::Duration) -> crate::AktorTimeout<Self> {
        crate::AktorTimeout::browser(self, duration, |request| crate::timeout::WaitStatus {
            admitted: request.reply.is_some(),
        })
    }

    pub fn send(self) -> WorkerRequestSend<'a, S, O, Role> {
        WorkerRequestSend(self)
    }
}
impl<S, O: 'static, Role> Future for WorkerRequest<'_, S, O, Role> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        ready!(this.poll_checked(cx))?;

        match this.reply.as_mut() {
            Some(reply) => Pin::new(reply).poll(cx),
            None => Poll::Pending,
        }
    }
}
impl<'a, S, Role> WorkerRequest<'a, S, (), Role> {
    pub fn cast(self) -> WorkerRequestCast<'a, S, Role> {
        WorkerRequestCast(self.send())
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WorkerRequestSend<'a, S, O, Role = ()>(WorkerRequest<'a, S, O, Role>);
impl<S, O, Role> Unpin for WorkerRequestSend<'_, S, O, Role> {}
impl<S, O: 'static, Role> Future for WorkerRequestSend<'_, S, O, Role> {
    type Output = Result<WorkerReply<O>, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let request = &mut self.get_mut().0;
        if let Err(error) = ready!(request.poll_checked(cx)) {
            return Poll::Ready(Err(error.into()));
        }

        Poll::Ready(request.reply.take().ok_or_else(|| {
            WireFailure::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Closed).into()
        }))
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WorkerRequestCast<'a, S, Role = ()>(WorkerRequestSend<'a, S, (), Role>);
impl<S, Role> Unpin for WorkerRequestCast<'_, S, Role> {}
impl<S, Role> Future for WorkerRequestCast<'_, S, Role> {
    type Output = Result<(), crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx).map_ok(drop)
    }
}
