use super::*;
use crate::AktorError;
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

        #[cfg(feature = "std")]
        self.inner.capture(self.operation.name, running).await;

        #[cfg(not(feature = "std"))]
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

pub async fn serve_on<Clock: clock::AktorGroupClock, S, const N: usize, E>(
    runner: AktorRunner<'_, S, N, E>,
) -> Result<(), crate::AktorError> {
    let _ = Clock::EXECUTION;

    #[cfg(all(
        feature = "browser_local",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    if Clock::EXECUTION == crate::AktorExecution::BrowserLocal {
        return serve_browser(runner).await;
    }

    serve(runner).await
}

#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
pub async fn serve_browser<S, const N: usize, E>(
    mut runner: AktorRunner<'_, S, N, E>,
) -> Result<(), crate::AktorError> {
    let mut remaining = 32;

    while let Some(call) = runner.next().await {
        call.run().await;
        remaining -= 1;

        if remaining == 0 {
            crate::timeout::browser_sleep(core::time::Duration::ZERO).await;
            remaining = 32;
        }
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
        self,
        setup: Setup,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), OwnerError>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, AktorSetupError<E>>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorCleanupError<E>>>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), crate::AktorError>,
    {
        self.run_prepared(setup, cleanup, runner).await
    }

    #[doc(hidden)]
    pub async fn run_prepared<Setup, SetupFuture, Cleanup, CleanupFuture, Runner>(
        mut self,
        setup: Setup,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), OwnerError>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future,
        SetupFuture::Output: LocalSetupOutput<S, E>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future,
        CleanupFuture::Output: LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), crate::AktorError>,
    {
        #[cfg(feature = "std")]
        let (initialized, mut primary) = match self
            .inner
            .capture_result("setup", async { setup().await.into_owner_setup() })
            .await
        {
            Ok(result) => (result, None),
            Err(payload) => {
                let error = crate::AktorError::new(crate::panic::panic_message(&payload), ());

                (Err(OwnerFailure::SetupPanic(error)), Some(payload))
            }
        };
        #[cfg(not(feature = "std"))]
        let initialized = setup().await.into_owner_setup();
        #[cfg(feature = "std")]
        let setup_panicked = primary.is_some();

        match initialized {
            Ok(state) => self.run_custom(state, cleanup, runner).await,
            Err(error) => {
                let error = Rc::new(error);

                if !self.inner.fail(&error)
                    && self.inner.group.borrow().is_some()
                    && let OwnerFailure::Setup(error) = &*error
                {
                    self.inner
                        .completion
                        .diagnostics
                        .borrow_mut()
                        .push(crate::AktorError::new(error.diagnostics.clone(), ()));
                }
                #[cfg(feature = "std")]
                {
                    let inner = self.inner.clone();

                    if let Err(error) = inner
                        .settle("hook drop", self.dispose_hooks(), &mut primary)
                        .await
                    {
                        inner.completion.diagnostics.borrow_mut().push(error);
                    }

                    if let Err(error) = inner
                        .settle(
                            "cleanup closure drop",
                            async { drop(cleanup) },
                            &mut primary,
                        )
                        .await
                    {
                        inner.completion.diagnostics.borrow_mut().push(error);
                    }

                    if let Err(error) = inner
                        .settle("runner closure drop", async { drop(runner) }, &mut primary)
                        .await
                    {
                        inner.completion.diagnostics.borrow_mut().push(error);
                    }

                    if !setup_panicked && let Some(payload) = primary.take() {
                        crate::panic::dispose_secondary(payload);
                    }
                }
                #[cfg(not(feature = "std"))]
                {
                    self.dispose_hooks().await;
                    drop((cleanup, runner));
                }

                #[cfg(feature = "std")]
                if !setup_panicked {
                    self.inner.completion.cleanup_completed.set(true);
                }
                #[cfg(not(feature = "std"))]
                self.inner.completion.cleanup_completed.set(true);
                let result = Err(error);

                #[cfg(feature = "std")]
                {
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

                    if let Some(payload) = primary {
                        std::panic::resume_unwind(payload);
                    }
                }
                #[cfg(not(feature = "std"))]
                self.inner.finish(result.clone());
                result.map_err(|err| {
                    SharedOwnerError::from_failure(err, &self.inner.completion.diagnostics.borrow())
                        .without_data()
                })
            }
        }
    }

    pub async fn run_custom<Cleanup, CleanupFuture, Runner>(
        mut self,
        mut state: S,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), OwnerError>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future,
        CleanupFuture::Output: LocalCleanupOutput<E>,
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
                        (),
                    )))
                } else {
                    result
                }
            })
            .await
        };

        #[cfg(feature = "std")]
        let mut primary = None;

        #[cfg(feature = "std")]
        let served = self
            .inner
            .settle("runner", serving, &mut primary)
            .await
            .and_then(|result| result);

        #[cfg(not(feature = "std"))]
        let served = serving.await;

        let result = served
            .and_then(|()| {
                if self.inner.open.get()
                    || !self.inner.queue.borrow().is_empty()
                    || !self.inner.services.borrow().is_empty()
                {
                    Err(crate::AktorError::new(
                        "custom runner stopped before draining admission",
                        (),
                    ))
                } else {
                    Ok(())
                }
            })
            .map_err(|error| Rc::new(OwnerFailure::Runner(error)));

        if let Err(error) = &result {
            self.inner.fail(error);
        }
        #[cfg(feature = "std")]
        if let Err(error) = self
            .inner
            .settle("close", async { self.inner.close() }, &mut primary)
            .await
        {
            self.inner.completion.diagnostics.borrow_mut().push(error);
        }
        #[cfg(not(feature = "std"))]
        self.inner.close();

        #[cfg(feature = "std")]
        if let Err(error) = self
            .inner
            .settle(
                "queue drop",
                async { self.inner.discard_queued() },
                &mut primary,
            )
            .await
        {
            self.inner.completion.diagnostics.borrow_mut().push(error);
        }
        #[cfg(not(feature = "std"))]
        self.inner.discard_queued();

        #[cfg(feature = "std")]
        let mut cleaned = self
            .inner
            .settle(
                "cleanup",
                async {
                    let result = cleanup(state).await.into_owner_cleanup();
                    self.inner.completion.cleanup_completed.set(true);
                    result
                },
                &mut primary,
            )
            .await
            .map_err(|error| Rc::new(OwnerFailure::Runner(error)))
            .and_then(|result| result.map_err(Rc::new));
        #[cfg(not(feature = "std"))]
        let mut cleaned = {
            let result = cleanup(state).await.into_owner_cleanup().map_err(Rc::new);
            self.inner.completion.cleanup_completed.set(true);
            result
        };

        if let Err(error) = &mut cleaned {
            let primary = self.inner.fail(error);

            if let Some(OwnerFailure::CleanupFailure(failure)) = Rc::get_mut(error) {
                failure.primary = primary;
            }
        }
        #[cfg(feature = "std")]
        {
            let inner = self.inner.clone();

            if let Err(error) = inner
                .settle("hook drop", self.dispose_hooks(), &mut primary)
                .await
            {
                inner.completion.diagnostics.borrow_mut().push(error);
            }
        }
        #[cfg(not(feature = "std"))]
        self.dispose_hooks().await;

        let retain_cleanup = result.is_err();
        #[cfg(feature = "std")]
        let retain_cleanup = retain_cleanup || primary.is_some();

        if retain_cleanup
            && let Err(error) = &cleaned
            && !matches!(&**error, OwnerFailure::CleanupFailure(failure) if failure.primary)
        {
            self.inner
                .completion
                .diagnostics
                .borrow_mut()
                .push(crate::AktorError::new(alloc::format!("{error:#}"), ()));
        }

        let result = result.and(cleaned);
        #[cfg(feature = "std")]
        let result = if let Some(payload) = &primary {
            Err(Rc::new(OwnerFailure::Runner(crate::AktorError::new(
                crate::panic::panic_message(payload),
                (),
            ))))
        } else {
            result
        };

        #[cfg(feature = "std")]
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

        #[cfg(not(feature = "std"))]
        self.inner.finish(result.clone());

        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }

        result.map_err(|err| {
            SharedOwnerError::from_failure(err, &self.inner.completion.diagnostics.borrow())
                .without_data()
        })
    }
}

pub trait LocalCleanupOutput<E> {
    fn into_owner_cleanup(self) -> Result<(), OwnerFailure<E>>;
}
impl<E> LocalCleanupOutput<E> for Result<(), AktorCleanupError<E>> {
    fn into_owner_cleanup(self) -> Result<(), OwnerFailure<E>> {
        self.map_err(OwnerFailure::Cleanup)
    }
}
impl LocalCleanupOutput<()> for Result<(), crate::AktorFailure> {
    fn into_owner_cleanup(self) -> Result<(), OwnerFailure<()>> {
        self.map_err(OwnerFailure::CleanupFailure)
    }
}

pub trait LocalSetupOutput<S, C> {
    fn into_owner_setup(self) -> Result<S, OwnerFailure<C>>;
}
impl<S, C> LocalSetupOutput<S, C> for Result<S, AktorSetupError<C>> {
    fn into_owner_setup(self) -> Result<S, OwnerFailure<C>> {
        self.map_err(OwnerFailure::Setup)
    }
}
impl<S, C> LocalSetupOutput<S, C>
    for (
        Result<S, crate::AktorFailure>,
        crate::setup::ready::Sender<Result<(), crate::AktorStartError<crate::AktorFailure>>>,
    )
{
    fn into_owner_setup(self) -> Result<S, OwnerFailure<C>> {
        let (result, ready) = self;

        match result {
            Ok(state) => {
                ready.send(Ok(()));
                Ok(state)
            }
            Err(error) => {
                let diagnostic = AktorError::new(error.diagnostics.clone(), ());
                let primary = error.primary;
                ready.send(Err(crate::AktorStartError::Init(error)));
                Err(OwnerFailure::SetupDiagnostic(diagnostic, primary))
            }
        }
    }
}
