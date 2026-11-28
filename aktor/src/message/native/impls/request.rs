use super::super::*;
use core::{future::poll_fn, task::ready};

impl<'a, S: 'static, O> Request<'a, S, O> {
    pub fn timeout(self, duration: core::time::Duration) -> crate::AktorTimeout<Self> {
        let standard = self.admission.is_standard();

        crate::AktorTimeout::queued(
            self,
            duration,
            |request| crate::timeout::WaitStatus {
                admitted: match request.submission {
                    Submission::Submitted | Submission::Consumed => true,
                    Submission::Unsent(_) | Submission::Waiting { .. } | Submission::Closed => {
                        false
                    }
                },
            },
            standard,
        )
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

    #[cfg(feature = "transport")]
    #[doc(hidden)]
    pub fn forward(mut self) -> Self {
        self.reply.receive = false;
        self
    }

    pub fn send(self) -> RequestSend<'a, S, O> {
        RequestSend(Some(self))
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        loop {
            if matches!(self.submission, Submission::Unsent(_)) {
                let old = std::mem::replace(&mut self.submission, Submission::Consumed);

                if let Submission::Unsent(message) = old {
                    let sender = self.sender;
                    if self.admission.phase().is_closing() {
                        self.submission = Submission::Closed;
                        drop(message);
                        return Poll::Ready(false);
                    }

                    if let Ok(permit) = sender.try_reserve() {
                        #[cfg(feature = "tokio")]
                        if core::pin::pin!(tokio::task::coop::consume_budget())
                            .poll(cx)
                            .is_pending()
                        {
                            self.submission = Submission::Unsent(message);
                            return Poll::Pending;
                        }

                        match self.admission.admit(permit, message) {
                            Ok(()) => {
                                self.submission = Submission::Submitted;
                                return Poll::Ready(true);
                            }
                            Err(message) => {
                                self.submission = Submission::Unsent(message);
                                continue;
                            }
                        }
                    }

                    self.submission = Submission::Waiting {
                        message,
                        admission: Box::pin(async move { sender.reserve().await.ok() }),
                    };
                }
            }

            let permit = match &mut self.submission {
                Submission::Waiting { admission, .. } => ready!(admission.as_mut().poll(cx)),
                Submission::Submitted => return Poll::Ready(true),
                Submission::Closed => return Poll::Ready(false),
                Submission::Consumed => consumed(),
                Submission::Unsent(_) => continue,
            };

            let old = std::mem::replace(&mut self.submission, Submission::Consumed);

            if let Submission::Waiting { message, .. } = old {
                match permit {
                    Some(permit) => match self.admission.admit(permit, message) {
                        Ok(()) => {
                            self.submission = Submission::Submitted;
                            return Poll::Ready(true);
                        }
                        Err(message) if !self.sender.is_closed() => {
                            self.submission = Submission::Unsent(message)
                        }
                        Err(message) => {
                            self.submission = Submission::Closed;
                            drop(message);
                            return Poll::Ready(false);
                        }
                    },
                    None if !self.sender.is_closed() => {
                        self.submission = Submission::Unsent(message)
                    }
                    None => {
                        self.submission = Submission::Closed;
                        drop(message);
                        return Poll::Ready(false);
                    }
                }
            }
        }
    }
}
impl<S: 'static, O> Future for Request<'_, S, O> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if !ready!(this.poll_submit(cx)) {
            return Poll::Ready(Err(this
                .admission
                .call_error(AktorDeliveryOutcome::NotAdmitted, this.reply.receive)));
        }

        let output = ready!(Pin::new(&mut this.reply).poll(cx));

        this.submission = Submission::Consumed;
        Poll::Ready(output)
    }
}
impl<'a, S: 'static> Request<'a, S, ()> {
    #[doc(hidden)]
    pub async fn run_interval(mut self) -> bool {
        let admitted = poll_fn(|cx| {
            if self.admission.phase().is_closing() || self.sender.is_closed() {
                return Poll::Ready(false);
            }

            self.poll_submit(cx)
        })
        .await;

        if admitted {
            return self.reply.await.is_ok();
        }

        admitted
    }

    /// Submit without waiting for completion.
    pub fn cast(self) -> RequestCast<'a, S> {
        RequestCast(self.send())
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RequestSend<'a, S, O>(Option<Request<'a, S, O>>);
impl<S, O> Unpin for RequestSend<'_, S, O> {}
impl<S: 'static, O> Future for RequestSend<'_, S, O> {
    type Output = Result<Reply<O>, crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(request) = &mut this.0 else {
            consumed();
        };

        let admitted = ready!(request.poll_submit(cx));
        let Some(request) = this.0.take() else {
            consumed();
        };

        if admitted {
            Poll::Ready(Ok(request.reply))
        } else {
            Poll::Ready(Err(request.admission.call_error(
                AktorDeliveryOutcome::NotAdmitted,
                request.reply.receive,
            )))
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RequestCast<'a, S>(RequestSend<'a, S, ()>);
impl<S> Unpin for RequestCast<'_, S> {}
impl<S: 'static> Future for RequestCast<'_, S> {
    type Output = Result<(), crate::AktorDeliveryError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx).map_ok(drop)
    }
}
