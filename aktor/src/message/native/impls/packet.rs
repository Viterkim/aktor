use super::super::*;

impl<F, I, O> Packet<F, I, O> {
    pub fn new(function: F, input: I, admission: Option<Arc<crate::queue::Admission>>) -> Self {
        Self {
            admission,
            data: Mutex::new(Data {
                function: Some((function, input)),
                completion: Completion::Waiting(None),
            }),
        }
    }

    pub fn finish(&self, output: Result<O, crate::CallLoss>) {
        let mut data = self.data.lock();

        match data.completion {
            Completion::Waiting(_) => {
                let old = std::mem::replace(&mut data.completion, Completion::Ready(output));

                drop(data);

                if let Completion::Waiting(Some(waker)) = old {
                    waker.wake();
                }
            }
            Completion::Ready(_) | Completion::Abandoned | Completion::Consumed => {
                drop(data);
                drop(output);
            }
        }
    }
}
impl<S, F, I, O> Job<S> for Packet<F, I, O>
where
    F: FnOnce(&mut S, I) -> O + Send,
    I: Send,
    O: Send,
{
    fn run<'a>(
        &'a self,
        state: &'a mut S,
        hooks: &'a mut crate::listener::hooks::CallHooks<S>,
        operation: crate::operation::Operation,
    ) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let function = self.data.lock().function.take();

            if let Some((function, input)) = function {
                hooks.before(state, operation);

                let output = function(state, input);

                hooks.after(state, operation);
                self.finish(Ok(output));
            }
        })
    }

    fn close(&self) {
        let function = self.data.lock().function.take();
        let error = if function.is_some() {
            AktorDeliveryOutcome::Discarded
        } else {
            AktorDeliveryOutcome::OutcomeUnknown
        };

        self.finish(Err(crate::CallLoss(
            error,
            self.admission.as_ref().and_then(|a| a.lost.get().cloned()),
        )));
        drop(function);
    }
}
impl<F: Send, I: Send, O: Send> Answer<O> for Packet<F, I, O> {
    fn try_take(&self) -> Option<Result<O, crate::CallLoss>> {
        let mut data = self.data.lock();

        match data.completion {
            Completion::Waiting(_) => return None,
            Completion::Ready(_) | Completion::Abandoned | Completion::Consumed => {}
        }

        let old = std::mem::replace(&mut data.completion, Completion::Consumed);

        drop(data);

        match old {
            Completion::Ready(output) => Some(output),
            Completion::Waiting(_) | Completion::Abandoned | Completion::Consumed => consumed(),
        }
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<O, crate::CallLoss>> {
        let mut data = self.data.lock();

        // One lock per poll. A waker clone is a count bump, the old one drops outside the lock.
        if let Completion::Waiting(waiting) = &mut data.completion {
            if waiting
                .as_ref()
                .is_some_and(|waker| waker.will_wake(cx.waker()))
            {
                return Poll::Pending;
            }

            let previous = waiting.replace(cx.waker().clone());

            drop(data);
            drop(previous);
            return Poll::Pending;
        }

        let old = std::mem::replace(&mut data.completion, Completion::Consumed);

        drop(data);

        match old {
            Completion::Ready(output) => Poll::Ready(output),
            Completion::Waiting(_) | Completion::Abandoned | Completion::Consumed => consumed(),
        }
    }

    fn abandon(&self) {
        let old = {
            let mut data = self.data.lock();
            std::mem::replace(&mut data.completion, Completion::Abandoned)
        };

        drop(old);
    }
}

impl<S, F, I, O> Job<S> for AsyncJob<F, I, O>
where
    F: for<'s> core::ops::AsyncFnOnce(&'s mut S, I) -> O + Send,
    I: Send,
    O: Send,
{
    fn run<'a>(
        &'a self,
        state: &'a mut S,
        hooks: &'a mut crate::listener::hooks::CallHooks<S>,
        operation: crate::operation::Operation,
    ) -> LocalFuture<'a, ()> {
        Box::pin(async move {
            let function = self.0.data.lock().function.take();

            if let Some((function, input)) = function {
                hooks.before(state, operation);

                let output = function(state, input).await;

                hooks.after(state, operation);
                self.0.finish(Ok(output));
            }
        })
    }

    fn close(&self) {
        let function = self.0.data.lock().function.take();
        let error = if function.is_some() {
            AktorDeliveryOutcome::Discarded
        } else {
            AktorDeliveryOutcome::OutcomeUnknown
        };

        self.0.finish(Err(crate::CallLoss(
            error,
            self.0
                .admission
                .as_ref()
                .and_then(|a| a.lost.get().cloned()),
        )));
        drop(function);
    }
}
impl<F: Send, I: Send, O: Send> Answer<O> for AsyncJob<F, I, O> {
    fn try_take(&self) -> Option<Result<O, crate::CallLoss>> {
        self.0.try_take()
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<O, crate::CallLoss>> {
        self.0.poll(cx)
    }

    fn abandon(&self) {
        self.0.abandon();
    }
}
