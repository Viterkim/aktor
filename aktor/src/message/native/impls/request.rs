use super::super::*;
use core::{future::poll_fn, task::ready};

impl<'a, S: 'static, O> Request<'a, S, O> {
    pub fn timeout(self, duration: core::time::Duration) -> crate::Timeout<Self> {
        let standard = self.admission.is_standard();

        crate::Timeout::queued(
            self,
            duration,
            |request| crate::timeout::WaitStatus {
                admitted: matches!(
                    request.submission,
                    Submission::Submitted | Submission::Consumed
                ),
                stopping: request
                    .admission
                    .group()
                    .is_some_and(|group| group.is_stopping()),
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

    /// Queue the call and keep its reply for later.
    pub async fn send(mut self) -> Reply<O> {
        if !poll_fn(|cx| self.poll_submit(cx)).await {
            if self.admission.group().is_some() {
                self.admission.rejected();
                core::future::pending::<()>().await;
            }

            if self.sender.is_closed() {
                self.reply.wait_closed().await;
            }

            stopped();
        }

        self.reply
    }

    fn poll_submit(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        loop {
            if let Some(available) = &mut self.available {
                ready!(available.as_mut().poll(cx));
                self.available = None;
            }

            if matches!(self.submission, Submission::Unsent(_)) {
                let old = std::mem::replace(&mut self.submission, Submission::Consumed);

                if let Submission::Unsent(message) = old {
                    let sender = self.sender;
                    let Some(epoch) = self.admission.epoch() else {
                        if sender.is_closed() {
                            self.submission = Submission::Closed;
                            drop(message);
                            return Poll::Ready(false);
                        }

                        let mut changed = self.admission.watch();
                        let mut finished = self.reply.finished.clone();
                        let admission = self.admission;

                        self.submission = Submission::Unsent(message);
                        self.available = Some(Box::pin(async move {
                            if admission.epoch().is_none() && !sender.is_closed() {
                                tokio::select! { _ = changed.changed() => (), _ = finished.changed() => () }
                            }
                        }));
                        continue;
                    };

                    if let Ok(permit) = sender.try_reserve() {
                        #[cfg(feature = "tokio")]
                        if core::pin::pin!(tokio::task::coop::consume_budget())
                            .poll(cx)
                            .is_pending()
                        {
                            self.submission = Submission::Unsent(message);
                            return Poll::Pending;
                        }

                        match self.admission.admit(epoch, permit, message) {
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

                    let mut changed = self.admission.watch();
                    let admission = self.admission;

                    self.submission = Submission::Waiting {
                        message,
                        admission: Box::pin(async move {
                            if admission.epoch() != Some(epoch) {
                                return Err(());
                            }

                            tokio::select! {
                                biased;
                                _ = changed.changed() => Err(()),
                                permit = sender.reserve() => permit.map_err(|_| ()),
                            }
                        }),
                        epoch,
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

            let old = std::mem::replace(&mut self.submission, Submission::Closed);

            if let Submission::Waiting { message, epoch, .. } = old {
                match permit {
                    Ok(permit) => match self.admission.admit(epoch, permit, message) {
                        Ok(()) => {
                            self.submission = Submission::Submitted;
                            return Poll::Ready(true);
                        }
                        Err(message) if !self.sender.is_closed() => {
                            self.submission = Submission::Unsent(message)
                        }
                        Err(_) => return Poll::Ready(false),
                    },
                    Err(_) if !self.sender.is_closed() => {
                        self.submission = Submission::Unsent(message)
                    }
                    Err(_) => return Poll::Ready(false),
                }
            }
        }
    }
}
impl<S: 'static, O> Future for Request<'_, S, O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();

        if !ready!(this.poll_submit(cx)) && this.admission.group().is_some() {
            this.admission.rejected();
            return Poll::Pending;
        }

        let output = ready!(Pin::new(&mut this.reply).poll(cx));

        this.submission = Submission::Consumed;
        Poll::Ready(output)
    }
}
impl<S: 'static> Request<'_, S, ()> {
    #[doc(hidden)]
    pub async fn run_interval(mut self) -> bool {
        let admitted = poll_fn(|cx| {
            if matches!(self.admission.phase(), crate::queue::Phase::Closing { .. })
                || self.sender.is_closed()
            {
                return Poll::Ready(false);
            }

            self.poll_submit(cx)
        })
        .await;

        if admitted {
            self.reply.await;
        }

        admitted
    }

    /// Submit without waiting for completion.
    pub async fn cast(self) {
        drop(self.send().await);
    }
}
