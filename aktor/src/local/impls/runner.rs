use super::*;
use alloc::string::ToString;
use core::{future::poll_fn, ops::AsyncFnOnce};

pub struct AktorRunner<'a, S, const N: usize = 0, E = ()> {
    pub state: &'a mut S,
    hooks: &'a mut hooks::AktorHooks<S>,
    inner: Rc<Inner<S, N, E>>,
    discarded: Rc<Cell<bool>>,
}
impl<S, const N: usize, E> AktorRunner<'_, S, N, E> {
    pub async fn next(&mut self) -> Option<AktorCustomCall<'_, S, N, E>> {
        let message = next(&self.inner).await?;

        Some(AktorCustomCall {
            inner: self.inner.clone(),
            discarded: self.discarded.clone(),
            completed: false,
            operation: message._operation,
            message,
            state: self.state,
            hooks: self.hooks,
        })
    }
}

#[must_use = "run the admitted call to deliver its reply"]
pub struct AktorCustomCall<'a, S, const N: usize = 0, E = ()> {
    inner: Rc<Inner<S, N, E>>,
    discarded: Rc<Cell<bool>>,
    completed: bool,
    pub operation: Operation,
    message: Message<S>,
    state: &'a mut S,
    hooks: &'a mut hooks::AktorHooks<S>,
}
impl<S, const N: usize, E> AktorCustomCall<'_, S, N, E> {
    pub async fn run(mut self) {
        let running = self.message.job.run(self.state, self.hooks, self.operation);
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        self.inner.capture(self.operation.name, running).await;
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        {
            let _inner = &self.inner;
            running.await;
        }
        self.completed = true;
    }
}
impl<S, const N: usize, E> Drop for AktorCustomCall<'_, S, N, E> {
    fn drop(&mut self) {
        if !self.completed {
            self.discarded.set(true);
            self.inner.closed.notify();
        }
    }
}

pub async fn serve<S, const N: usize, E>(
    mut runner: AktorRunner<'_, S, N, E>,
) -> Result<(), crate::AktorError> {
    while let Some(call) = runner.next().await {
        call.run().await;

        let mut yielded = false;

        poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    Ok(())
}

async fn next<S, const N: usize, E>(inner: &Inner<S, N, E>) -> Option<Message<S>> {
    let changed = inner.closed.listen();

    poll_fn(|cx| {
        changed.register(cx);

        if !inner.open.get()
            && inner.queue.borrow().is_empty()
            && inner.services.borrow().is_empty()
        {
            return Poll::Ready(None);
        }

        if (inner.prefer_service.get() || inner.queue.borrow().is_empty())
            && let Some(message) = inner.services.borrow_mut().pop_front()
        {
            inner.prefer_service.set(false);
            return Poll::Ready(Some(message));
        }

        let message = inner.queue.borrow_mut().pop_front();

        match message {
            Some(message) => {
                inner.prefer_service.set(true);
                inner.closed.notify();
                Poll::Ready(Some(message))
            }
            None => Poll::Pending,
        }
    })
    .await
}

impl<S, const N: usize, E> Owner<S, N, E> {
    pub async fn run_with_custom<Setup, SetupFuture, Cleanup, CleanupFuture, Runner>(
        mut self,
        setup: Setup,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), Rc<OwnerError<E>>>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, AktorSetupError<E>>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), crate::AktorError>,
    {
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let initialized = self.inner.capture("setup", async { setup().await }).await;
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        let initialized = setup().await;

        match initialized {
            Ok(state) => self.run_custom(state, cleanup, runner).await,
            Err(error) => {
                let error = Rc::new(OwnerError::Setup(error));

                self.inner.fail(&error);
                #[cfg(any(
                    feature = "tokio",
                    all(feature = "std_thread", not(target_family = "wasm"))
                ))]
                {
                    let inner = self.inner.clone();
                    let mut primary = None;

                    if let Err(error) = inner
                        .settle("hook drop", self.dispose_hooks(), &mut primary)
                        .await
                    {
                        inner.completion.diagnostics.borrow_mut().push(error);
                    }

                    if let Some(payload) = primary {
                        crate::listener::failure::dispose_secondary(payload);
                    }
                }
                #[cfg(not(any(
                    feature = "tokio",
                    all(feature = "std_thread", not(target_family = "wasm"))
                )))]
                self.dispose_hooks().await;

                let result = Err(error);

                self.inner.finish(result.clone());
                result
            }
        }
    }

    pub async fn run_custom<Cleanup, CleanupFuture, Runner>(
        mut self,
        mut state: S,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), Rc<OwnerError<E>>>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), crate::AktorError>,
    {
        *self.inner.completion.ready.borrow_mut() = Some(Ok(()));
        self.inner.completion.changed.notify();

        let discarded = Rc::new(Cell::new(false));
        let inner = self.inner.clone();
        let changed = inner.closed.listen();
        let serving = async {
            let serving = runner(AktorRunner {
                state: &mut state,
                hooks: &mut self.hooks,
                inner: inner.clone(),
                discarded: discarded.clone(),
            });

            let mut serving = core::pin::pin!(serving);

            poll_fn(|cx| {
                changed.register(cx);

                let result = serving.as_mut().poll(cx);

                if discarded.get() {
                    Poll::Ready(Err(crate::AktorError::new(
                        "custom runner discarded admitted work",
                    )))
                } else {
                    result
                }
            })
            .await
        };
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let mut primary = None;
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let served = self
            .inner
            .settle("runner", serving, &mut primary)
            .await
            .and_then(|result| result);
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        let served = serving.await;

        let result = served
            .and_then(|()| {
                if self.inner.open.get()
                    || !self.inner.queue.borrow().is_empty()
                    || !self.inner.services.borrow().is_empty()
                {
                    Err(crate::AktorError::new(
                        "custom runner stopped before draining admission",
                    ))
                } else {
                    Ok(())
                }
            })
            .map_err(|error| Rc::new(OwnerError::Runner(error)));

        if let Err(error) = &result {
            self.inner.fail(error);
        }
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        if let Err(error) = self
            .inner
            .settle("close", async { self.inner.close() }, &mut primary)
            .await
        {
            self.inner.completion.diagnostics.borrow_mut().push(error);
        }
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        self.inner.close();

        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let cleaned = self
            .inner
            .settle("cleanup", async { cleanup(state).await }, &mut primary)
            .await
            .map_err(|error| Rc::new(OwnerError::Runner(error)))
            .and_then(|result| result.map_err(|error| Rc::new(OwnerError::Cleanup(error))));
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        let cleaned = cleanup(state)
            .await
            .map_err(|error| Rc::new(OwnerError::Cleanup(error)));

        if let Err(error) = &cleaned {
            self.inner.fail(error);
        }
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        {
            let inner = self.inner.clone();

            if let Err(error) = inner
                .settle("hook drop", self.dispose_hooks(), &mut primary)
                .await
            {
                inner.completion.diagnostics.borrow_mut().push(error);
            }
        }
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        self.dispose_hooks().await;

        let retain_cleanup = result.is_err();
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let retain_cleanup = retain_cleanup || primary.is_some();

        if retain_cleanup && let Err(error) = &cleaned {
            self.inner
                .completion
                .diagnostics
                .borrow_mut()
                .push(crate::AktorError::new(error.to_string()));
        }

        let result = result.and(cleaned);
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let result = if let Some(payload) = &primary {
            Err(Rc::new(OwnerError::Runner(crate::AktorError::new(
                crate::group::shutdown::panic_message(payload),
            ))))
        } else {
            result
        };
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        if let Err(error) = self
            .inner
            .settle(
                "queue drop",
                async {
                    self.inner.finish(result.clone());
                },
                &mut primary,
            )
            .await
        {
            self.inner.completion.diagnostics.borrow_mut().push(error);
        }
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        self.inner.finish(result.clone());
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }

        result
    }
}
