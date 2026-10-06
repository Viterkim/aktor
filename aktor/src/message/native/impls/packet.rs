use super::super::*;

impl<F, I, O> Packet<F, I, O> {
    pub fn new(function: F, input: I) -> Self {
        Self {
            data: Mutex::new(Data {
                function: Some((function, input)),
                completion: Completion::Waiting(None),
            }),
        }
    }

    pub fn finish(&self, output: Result<O, CallError>) {
        let mut data = self.data.lock();

        if matches!(data.completion, Completion::Waiting(_)) {
            let old = std::mem::replace(&mut data.completion, Completion::Ready(output));

            drop(data);

            if let Completion::Waiting(Some(waker)) = old {
                waker.wake();
            }
        } else {
            drop(data);
            drop(output);
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
        hooks: &'a mut crate::listener::hooks::AktorHooks<S>,
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
            CallError::Discarded
        } else {
            CallError::OutcomeUnknown
        };

        self.finish(Err(error));
        drop(function);
    }
}
impl<F: Send, I: Send, O: Send> Answer<O> for Packet<F, I, O> {
    fn try_take(&self) -> Option<Result<O, CallError>> {
        let mut data = self.data.lock();

        if matches!(data.completion, Completion::Waiting(_)) {
            return None;
        }

        let old = std::mem::replace(&mut data.completion, Completion::Consumed);

        drop(data);

        match old {
            Completion::Ready(output) => Some(output),
            _ => consumed(),
        }
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<O, CallError>> {
        let next = cx.waker().clone();
        let mut data = self.data.lock();
        let old = std::mem::replace(&mut data.completion, Completion::Consumed);

        match old {
            Completion::Ready(output) => {
                drop(data);
                drop(next);
                Poll::Ready(output)
            }
            Completion::Waiting(previous) => {
                data.completion = Completion::Waiting(Some(next));
                drop(data);
                drop(previous);
                Poll::Pending
            }
            _ => {
                drop(data);
                drop(next);
                consumed()
            }
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
        hooks: &'a mut crate::listener::hooks::AktorHooks<S>,
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
            CallError::Discarded
        } else {
            CallError::OutcomeUnknown
        };

        self.0.finish(Err(error));
        drop(function);
    }
}
impl<F: Send, I: Send, O: Send> Answer<O> for AsyncJob<F, I, O> {
    fn try_take(&self) -> Option<Result<O, CallError>> {
        self.0.try_take()
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<O, CallError>> {
        self.0.poll(cx)
    }

    fn abandon(&self) {
        self.0.abandon();
    }
}
