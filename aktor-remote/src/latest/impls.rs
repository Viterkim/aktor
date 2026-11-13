use super::*;
use futures_core::Stream;

impl<I> Clone for LatestSender<I> {
    fn clone(&self) -> Self {
        self.input.clone_sender();
        Self {
            input: self.input.clone(),
        }
    }
}
impl<I> Drop for LatestSender<I> {
    fn drop(&mut self) {
        self.input.close_sender();
    }
}
impl<I> SendLatest<I> for LatestSender<I> {
    fn send(&self, input: I) {
        self.input.send(input);
    }
}

impl<I, O> LatestResults<I, O> {
    pub async fn next(&mut self) -> Option<Result<O, (Error, String)>> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    #[cfg(test)]
    pub async fn seen(&self, revision: u64) {
        self.shared
            .seen
            .subscribe()
            .wait_for(|seen| *seen >= revision)
            .await
            .unwrap();
    }

    #[cfg(test)]
    pub fn buffered(&self) -> bool {
        self.shared.lock().output.is_some()
    }
}
impl<I, O> futures_core::Stream for LatestResults<I, O> {
    type Item = Result<O, (Error, String)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let replacement = cx.waker().clone();
        if let Some(driver) = self.driver.as_mut()
            && let Poll::Ready(result) = Pin::new(driver).poll(cx)
        {
            self.driver = None;
            if let Err(error) = result {
                self.shared.fail(Failure::new(
                    CallError::NotAdmitted,
                    Cause::Closed,
                    error.to_string(),
                ));
            }
        }

        let mut slot = self.shared.lock();

        if let Some(output) = slot.output.take() {
            return Poll::Ready(Some(output.map_err(Into::into)));
        }
        if slot.finished {
            return Poll::Ready(None);
        }

        let previous = slot.wake.replace(replacement);
        drop(slot);
        drop(previous);
        Poll::Pending
    }
}
impl<I, O> Drop for LatestResults<I, O> {
    fn drop(&mut self) {
        let discarded = {
            let mut slot = self.shared.lock();
            slot.receiver = false;
            (slot.pending.take(), slot.output.take())
        };
        self.shared.changed.notify_one();
        self.shared.cancel.notify_one();
        drop(discarded);
    }
}

impl<I: Send, O: Send> Input<I> for Inputs<I, O> {
    fn send(&self, input: I) {
        let (pending, output, wake) = {
            let mut slot = self.shared.lock();
            if !slot.receiver || slot.finished {
                return;
            }

            let Some(revision) = slot.revision.checked_add(1) else {
                return;
            };
            slot.revision = revision;
            let options = slot.options;
            match options.deadline(Instant::now()) {
                Ok(deadline) => {
                    let update = Pending {
                        input: Some(input),
                        update: Update {
                            revision,
                            input: None,
                            deadline,
                            outcome: CallError::NotAdmitted,
                            uncertain: false,
                            attempts: 0,
                            options,
                        },
                    };
                    (slot.pending.replace(update), slot.output.take(), None)
                }
                Err(error) => (
                    slot.pending.take(),
                    slot.output.replace(Err(error)),
                    slot.wake.take(),
                ),
            }
        };
        self.shared.changed.notify_one();
        if let Some(wake) = wake {
            wake.wake();
        }
        drop((pending, output));
    }

    fn configure(&self, options: AktorRetryOpts) {
        self.shared.lock().options = options;
    }

    fn clone_sender(&self) {
        self.shared.lock().senders += 1;
    }

    fn close_sender(&self) {
        self.shared.lock().senders -= 1;
        self.shared.changed.notify_one();
    }
}

impl<I, O> Shared<I, O> {
    pub fn encode(
        &self,
        revision: u64,
        encode: fn(&I) -> Result<Vec<u8>, Failure>,
    ) -> Option<Update> {
        let input = {
            let mut slot = self.lock();
            let pending = slot.pending.as_mut()?;
            if pending.update.revision != revision {
                return None;
            }

            match pending.input.take() {
                Some(input) => input,
                None => return Some(pending.update.clone()),
            }
        };
        let encoded = encode(&input).map(Arc::from);

        let mut slot = self.lock();
        let pending = slot.pending.as_mut()?;
        if pending.update.revision != revision {
            return None;
        }

        pending.update.input = Some(encoded);
        Some(pending.update.clone())
    }

    pub fn attempt(&self, revision: u64) {
        if let Some(pending) = &mut self.lock().pending
            && pending.update.revision == revision
        {
            pending.update.attempts = pending.update.attempts.max(1);
        }
    }

    pub fn retry(&self, revision: u64) {
        let mut slot = self.lock();
        let Some(pending) = slot
            .pending
            .as_mut()
            .filter(|pending| pending.update.revision == revision)
        else {
            return;
        };

        let update = &mut pending.update;
        update.uncertain |= update.outcome == CallError::OutcomeUnknown;
        match update.options.deadline(Instant::now()) {
            Ok(deadline) => {
                update.attempts += 1;
                update.deadline = deadline;
            }
            Err(error) => {
                drop(slot);
                self.publish(revision, Err(error));
            }
        }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, Slot<I, O>> {
        self.slot.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub fn pending(&self) -> Option<Update> {
        self.lock()
            .pending
            .as_ref()
            .map(|pending| pending.update.clone())
    }

    pub fn closing(&self) -> bool {
        let slot = self.lock();
        !slot.receiver || (slot.senders == 0 && slot.pending.is_none())
    }

    pub fn publish(&self, revision: u64, mut output: Result<O, Failure>) {
        let mut slot = self.lock();
        if !slot.receiver || slot.revision != revision || slot.pending.is_none() {
            return;
        }

        if let Some(pending) = &slot.pending
            && pending.update.uncertain
            && let Err(error) = &mut output
        {
            error.outcome = CallError::OutcomeUnknown;
        }

        let pending = slot.pending.take();
        let discarded = slot.output.replace(output);
        let wake = slot.wake.take();
        drop(slot);
        drop((pending, discarded));
        self.changed.notify_one();
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    pub fn finish(&self) {
        let mut slot = self.lock();
        slot.finished = true;
        let wake = slot.wake.take();
        drop(slot);

        if let Some(wake) = wake {
            wake.wake();
        }
    }

    pub fn fail(&self, mut error: Failure) {
        let mut slot = self.lock();
        let pending = slot.pending.take();
        if let Some(pending) = &pending {
            error.outcome = if pending.update.uncertain {
                CallError::OutcomeUnknown
            } else {
                pending.update.outcome
            };
        }

        let discarded = slot.output.replace(Err(error));
        slot.finished = true;
        let wake = slot.wake.take();
        drop(slot);

        drop((pending, discarded));
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl<S, I: Send + 'static, O: Send + 'static, Lease, Role, C> TypedSession<S, I, O, Lease, Role, C>
    for &Client<S, Role>
where
    C: Codec<I> + Codec<O>,
{
    type Sender<Fut> = LatestSender<I>;
    type Results = LatestResults<I, O>;

    fn session<F, Fut>(
        self,
        operation: Operation,
        _: F,
        _: fn(Lease, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: Future<Output = (Lease, O)>,
    {
        start::<I, O, C>(self.inner.clone(), operation)
    }
}

pub fn start<I: Send + 'static, O: Send + 'static, C: Codec<I> + Codec<O>>(
    inner: Arc<Inner>,
    operation: Operation,
) -> (LatestSender<I>, LatestResults<I, O>) {
    let shared = Arc::new(Shared {
        slot: Mutex::new(Slot {
            revision: 0,
            pending: None,
            output: None,
            senders: 1,
            receiver: true,
            finished: false,
            wake: None,
            options: AktorRetryOpts {
                attempts: 1,
                timeout: inner.options.timeout,
            },
        }),
        changed: Notify::new(),
        cancel: Notify::new(),
        seen: watch::channel(0).0,
    });
    let input = Arc::new(Inputs {
        shared: shared.clone(),
    });
    let driver = match tokio::runtime::Handle::try_current() {
        Ok(runtime) => Some(runtime.spawn(driver::drive(
            inner,
            shared.clone(),
            operation,
            |input| C::encode(input).map_err(Into::into),
            C::decode_output,
        ))),
        Err(_) => {
            shared.fail(Failure::new(
                CallError::NotAdmitted,
                Cause::Closed,
                "remote latest needs a Tokio runtime",
            ));
            None
        }
    };
    (LatestSender { input }, LatestResults { shared, driver })
}

impl<I> aktor::call::RetryRequest for LatestSender<I> {
    type Options = AktorRetryOpts;

    fn retry(self, options: Self::Options) -> Self {
        self.input.configure(options);
        self
    }
}
