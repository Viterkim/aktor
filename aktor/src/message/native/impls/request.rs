use super::super::*;
use core::{future::poll_fn, task::ready};

impl<'a, S: 'static, O> Request<'a, S, O> {
    pub fn checked(self) -> CheckedRequest<'a, S, O> {
        CheckedRequest(self)
    }

    pub fn operation(mut self, operation: crate::listener::Operation) -> Self {
        match &mut self.submission {
            Submission::Unsent(message) | Submission::Waiting { message, .. } => {
                message.operation = operation
            }
            _ => {}
        }

        self
    }

    /// Replace older queued work for this operation and key. Running work finishes.
    pub fn latest<K: Eq + Send + Sync + 'static>(mut self, key: K) -> Self {
        if let Submission::Unsent(message) | Submission::Waiting { message, .. } =
            &mut self.submission
        {
            message.latest = Some(LatestKey {
                operation: message.operation.name,
                value: Arc::new(key),
            });
        }

        self
    }

    /// Submit once, waiting for capacity, and keep the result for later.
    pub async fn send(mut self) -> Reply<O> {
        if !poll_fn(|cx| self.poll_submit(cx)).await {
            if self.sender.is_closed() {
                self.reply.wait_closed().await;
            }
            stopped();
        }

        self.reply
    }

    pub async fn checked_send(mut self) -> Result<CheckedReply<O>, CallError> {
        if !poll_fn(|cx| self.poll_submit(cx)).await {
            return Err(CallError::NotAdmitted);
        }

        Ok(self.reply.checked())
    }

    /// Submit immediately. A pending reservation is cancelled first, giving up its queue position.
    #[allow(clippy::result_large_err)]
    pub fn try_send(mut self) -> Result<Reply<O>, TrySendError<Self>> {
        let pending = std::mem::replace(&mut self.submission, Submission::Consumed);
        let message = match pending {
            Submission::Unsent(message) => message,
            Submission::Waiting {
                message, admission, ..
            } => {
                drop(admission);
                message
            }
            Submission::Submitted => return Ok(self.reply),
            Submission::Closed => {
                self.submission = Submission::Closed;
                return Err(TrySendError::Closed(self));
            }
            Submission::Consumed => consumed(),
        };

        let Some(epoch) = self.admission.epoch() else {
            self.submission = Submission::Unsent(message);
            return Err(TrySendError::Closed(self));
        };

        let message = match self.admission.replace(epoch, self.sender, message) {
            Ok(()) => {
                self.submission = Submission::Submitted;
                return Ok(self.reply);
            }
            Err(message) => message,
        };

        match self.sender.try_reserve() {
            Ok(permit) => match self.admission.admit(epoch, permit, message) {
                Ok(()) => {
                    self.submission = Submission::Submitted;
                    Ok(self.reply)
                }
                Err(message) => {
                    self.submission = Submission::Unsent(message);
                    Err(TrySendError::Closed(self))
                }
            },
            Err(error) => {
                self.submission = Submission::Unsent(message);
                match error {
                    mpsc::error::TrySendError::Full(()) => Err(TrySendError::Full(self)),
                    mpsc::error::TrySendError::Closed(()) => Err(TrySendError::Closed(self)),
                }
            }
        }
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if matches!(self.submission, Submission::Unsent(_)) {
            let old = std::mem::replace(&mut self.submission, Submission::Consumed);
            if let Submission::Unsent(message) = old {
                let mut changed = self.admission.watch();
                let sender = self.sender;
                let mut queued = sender.watch();

                let Some(epoch) = self.admission.epoch() else {
                    self.submission = Submission::Closed;
                    drop(message);
                    return Poll::Ready(false);
                };

                let message = match self.admission.replace(epoch, sender, message) {
                    Ok(()) => {
                        self.submission = Submission::Submitted;
                        return Poll::Ready(true);
                    }
                    Err(message) => message,
                };

                self.submission = Submission::Waiting {
                    message,
                    admission: Box::pin(async move {
                        let reserved = sender.reserve();
                        tokio::pin!(reserved);

                        loop {
                            tokio::select! {
                                biased;
                                _ = changed.changed() => return Err(()),
                                // Let poll_submit look for a replacement without losing our place.
                                _ = queued.changed() => tokio::task::yield_now().await,
                                permit = &mut reserved => return permit.map_err(|_| ()),
                            }
                        }
                    }),
                    epoch,
                };
            }
        }

        if matches!(self.submission, Submission::Waiting { .. }) {
            let old = std::mem::replace(&mut self.submission, Submission::Consumed);
            if let Submission::Waiting {
                message,
                admission,
                epoch,
            } = old
            {
                self.submission = match self.admission.replace(epoch, self.sender, message) {
                    Ok(()) => Submission::Submitted,
                    Err(message) => Submission::Waiting {
                        message,
                        admission,
                        epoch,
                    },
                };
            }
        }

        let permit = match &mut self.submission {
            Submission::Waiting { admission, .. } => ready!(admission.as_mut().poll(cx)),
            Submission::Submitted => return Poll::Ready(true),
            Submission::Closed => return Poll::Ready(false),
            Submission::Consumed => consumed(),
            Submission::Unsent(_) => return Poll::Pending,
        };

        let old = std::mem::replace(&mut self.submission, Submission::Closed);
        if let Submission::Waiting { message, epoch, .. } = old {
            match permit {
                Ok(permit) => {
                    if self.admission.admit(epoch, permit, message).is_ok() {
                        self.submission = Submission::Submitted;
                        return Poll::Ready(true);
                    }
                }
                Err(_) => drop(message),
            }
        }

        Poll::Ready(false)
    }
}
impl<S: 'static, O> Future for Request<'_, S, O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if !ready!(this.poll_submit(cx)) && !this.sender.is_closed() {
            stopped();
        }

        let output = ready!(Pin::new(&mut this.reply).poll(cx));
        this.submission = Submission::Consumed;
        Poll::Ready(output)
    }
}
impl<S: 'static, O> Future for CheckedRequest<'_, S, O> {
    type Output = Result<O, CallError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut self.get_mut().0;

        if !ready!(this.poll_submit(cx)) {
            this.submission = Submission::Consumed;
            return Poll::Ready(Err(CallError::NotAdmitted));
        }

        let output = ready!(this.reply.poll_checked_result(cx));
        this.submission = Submission::Consumed;
        Poll::Ready(output)
    }
}
impl<S: 'static> Request<'_, S, ()> {
    /// Submit without waiting for completion.
    pub async fn cast(self) {
        drop(self.send().await);
    }

    pub async fn checked_cast(self) -> Result<(), CallError> {
        drop(self.checked_send().await?);
        Ok(())
    }

    /// Submit immediately, retaining the request on failure.
    #[allow(clippy::result_large_err)]
    pub fn try_cast(self) -> Result<(), TrySendError<Self>> {
        self.try_send().map(drop)
    }
}
