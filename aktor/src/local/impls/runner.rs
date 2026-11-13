use super::*;
use crate::AktorError;
use core::{future::poll_fn, ops::AsyncFnOnce};

pub struct AktorRunner<'a, S, const N: usize = 0, E = ()> {
    pub state: &'a mut S,
    hooks: &'a mut hooks::CallHooks<S>,
    inner: Rc<Inner<S, N, E>>,
    discarded: Rc<Cell<bool>>,
    waiting: &'a Cell<bool>,
}
impl<S, const N: usize, E> AktorRunner<'_, S, N, E> {
    pub async fn next(&mut self) -> Option<AktorCustomCall<'_, S, N, E>> {
        let _waiting = Waiting::new(self.waiting);
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

struct Waiting<'a>(&'a Cell<bool>);
impl<'a> Waiting<'a> {
    fn new(waiting: &'a Cell<bool>) -> Self {
        waiting.set(true);
        Self(waiting)
    }
}
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.set(false);
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
    hooks: &'a mut hooks::CallHooks<S>,
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
    let waiting = runner.waiting;
    let mut remaining = 32;

    loop {
        let Some(call) = runner.next().await else {
            return Ok(());
        };
        call.run().await;
        remaining -= 1;
        if remaining != 0 {
            continue;
        }

        remaining = 32;
        if !runner.inner.has_work() {
            continue;
        }
        let _waiting = Waiting::new(waiting);
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
    let waiting = runner.waiting;
    let mut remaining = 32;

    loop {
        let Some(call) = runner.next().await else {
            return Ok(());
        };
        call.run().await;
        remaining -= 1;

        if remaining == 0 {
            remaining = 32;
            if runner.inner.has_work() {
                let _waiting = Waiting::new(waiting);
                crate::timeout::browser_sleep(core::time::Duration::ZERO).await;
            }
        }
    }
}

async fn next<S, const N: usize, E>(inner: &Inner<S, N, E>) -> Option<Message<S>> {
    let waiting = OwnerWait(&inner.owner_waker);
    poll_fn(|cx| {
        let next = try_next(inner);
        if next.is_pending() && event::register(waiting.0, cx.waker()) {
            // Waker callbacks can enqueue work or close the actor.
            return try_next(inner);
        }
        next
    })
    .await
}

struct OwnerWait<'a>(&'a RefCell<Option<Waker>>);
impl Drop for OwnerWait<'_> {
    fn drop(&mut self) {
        drop(self.0.take());
    }
}

fn try_next<S, const N: usize, E>(inner: &Inner<S, N, E>) -> Poll<Option<Message<S>>> {
    if !inner.open.get() && !inner.has_work() {
        return Poll::Ready(None);
    }

    if (inner.prefer_service.get() || inner.queue.borrow().is_empty())
        && let Some(message) = inner.services.borrow_mut().pop_front()
    {
        inner.prefer_service.set(false);
        return Poll::Ready(Some(message));
    }

    let (message, was_full) = {
        let mut queue = inner.queue.borrow_mut();
        let was_full = queue.len() == inner.capacity;
        (queue.pop_front(), was_full)
    };

    match message {
        Some(message) => {
            inner.prefer_service.set(true);
            if was_full {
                inner.space.notify();
            }
            Poll::Ready(Some(message))
        }
        None => Poll::Pending,
    }
}

impl<S, const N: usize, E> Owner<S, N, E> {
    pub async fn run_with_custom<Setup, SetupFuture, Cleanup, CleanupFuture, Runner>(
        self,
        setup: Setup,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), (OwnerError, String)>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, AktorError<E>>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), AktorError<E>>>,
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
    ) -> Result<(), (OwnerError, String)>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future,
        SetupFuture::Output: LocalSetupOutput<S, E>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future,
        CleanupFuture::Output: LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), crate::AktorError>,
    {
        // Shutdown giving up on this actor cancels its calls, a `start` still running stops there too.
        let inner = self.inner.clone();
        let changed = inner.closed.listen();
        let starting = async {
            let mut starting = core::pin::pin!(async { setup().await.into_owner_setup() });

            poll_fn(|cx| {
                changed.register(cx);

                if inner.cancelled.get() {
                    return Poll::Ready(Err(OwnerError::Cancelled));
                }

                starting.as_mut().poll(cx)
            })
            .await
        };

        #[cfg(feature = "std")]
        let (initialized, mut primary) =
            match self.inner.capture_result("setup", Box::pin(starting)).await {
                Ok(result) => (result, None),
                Err(payload) => {
                    let error = crate::AktorError::new(crate::panic::panic_message(&payload), ());

                    (Err(OwnerError::SetupPanic(error)), Some(payload))
                }
            };
        #[cfg(not(feature = "std"))]
        let initialized = starting.await;
        #[cfg(feature = "std")]
        let setup_panicked = primary.is_some();

        match initialized {
            Ok(state) => self.run_custom(state, cleanup, runner).await,
            Err(error) => {
                let error = Rc::new(error);

                if !self.inner.fail(&error)
                    && self.inner.group.borrow().is_some()
                    && let OwnerError::Setup(error) = &*error
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

                if matches!(&*error, OwnerError::Cancelled) {
                    self.inner.completion.cut_call.set(true);
                } else if !matches!(&*error, OwnerError::SetupPanic(_)) {
                    self.inner.completion.cleanup_completed.set(true);
                }
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
                    let (error, report) = SharedOwnerError::from_failure(
                        err,
                        &self.inner.completion.diagnostics.borrow(),
                    );
                    (error.without_data(), report)
                })
            }
        }
    }

    pub async fn run_custom<Cleanup, CleanupFuture, Runner>(
        mut self,
        mut state: S,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), (OwnerError, String)>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future,
        CleanupFuture::Output: LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, N, E>) -> Result<(), crate::AktorError>,
    {
        *self.inner.completion.ready.borrow_mut() = Some(Ok(()));
        self.inner.completion.changed.notify();

        let discarded = Rc::new(Cell::new(false));
        let waiting = Cell::new(false);
        let inner = self.inner.clone();
        let changed = inner.closed.listen();
        let serving = async {
            let serving = runner(AktorRunner {
                state: &mut state,
                hooks: &mut self.hooks,
                inner: inner.clone(),
                discarded: discarded.clone(),
                waiting: &waiting,
            });

            let mut serving = core::pin::pin!(serving);
            let mut polled = false;

            poll_fn(|cx| {
                changed.register(cx);

                if inner.cancelled.get() {
                    inner.completion.cut_call.set(polled && !waiting.get());
                    return Poll::Ready(Ok(()));
                }

                polled = true;
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
                let undrained = self.inner.open.get()
                    || !self.inner.queue.borrow().is_empty()
                    || !self.inner.services.borrow().is_empty();

                // Cancelled calls are dropped on purpose, only a runner that quit early failed.
                if undrained && !self.inner.cancelled.get() {
                    Err(crate::AktorError::new(
                        "custom runner stopped before draining admission",
                        (),
                    ))
                } else {
                    Ok(())
                }
            })
            .map_err(|error| Rc::new(OwnerError::Runner(error)));

        // A panicking runner was reported when it panicked.
        #[cfg(feature = "std")]
        let panicked = primary.is_some();
        #[cfg(not(feature = "std"))]
        let panicked = false;

        if let Err(error) = &result
            && !panicked
        {
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

        let group = self
            .inner
            .group
            .borrow()
            .as_ref()
            .map(|(_, signal)| signal.clone());
        let ending = async {
            let end = cleanup(state);

            // In a group `end` gets its own grace, so one parked on an await can't hang shutdown.
            let output = match &group {
                Some(signal) => signal.end_within(end).await,
                None => Some(end.await),
            };

            let Some(output) = output else {
                self.inner.completion.ran_out.set(true);
                self.inner
                    .completion
                    .diagnostics
                    .borrow_mut()
                    .push(crate::AktorError::new("end ran out of its grace", ()));
                return Err(OwnerError::CleanupUnconfirmed);
            };

            let result = output.into_owner_cleanup();
            self.inner.completion.cleanup_completed.set(true);
            result
        };

        #[cfg(feature = "std")]
        let mut cleaned = self
            .inner
            .settle("cleanup", ending, &mut primary)
            .await
            .map_err(|error| Rc::new(OwnerError::Runner(error)))
            .and_then(|result| result.map_err(Rc::new));
        #[cfg(not(feature = "std"))]
        let mut cleaned = ending.await.map_err(Rc::new);

        if let Err(error) = &mut cleaned {
            let primary = self.inner.fail(error);

            if let Some(OwnerError::CleanupFailure(failure)) = Rc::get_mut(error) {
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
            && !matches!(&**error, OwnerError::CleanupFailure(failure) if failure.primary)
            && !matches!(&**error, OwnerError::CleanupUnconfirmed)
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
            let error = Rc::new(OwnerError::Runner(crate::AktorError::new(
                crate::panic::panic_message(payload),
                (),
            )));

            // When the panic is the group's failure, the outcome doesn't print it again.
            if self.inner.fail(&error) {
                self.inner.completion.panic_reported.set(true);
            }

            Err(error)
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
            let (error, report) =
                SharedOwnerError::from_failure(err, &self.inner.completion.diagnostics.borrow());
            (error.without_data(), report)
        })
    }
}

pub trait LocalCleanupOutput<E> {
    fn into_owner_cleanup(self) -> Result<(), OwnerError<E>>;
}
impl<E> LocalCleanupOutput<E> for Result<(), AktorError<E>> {
    fn into_owner_cleanup(self) -> Result<(), OwnerError<E>> {
        self.map_err(OwnerError::Cleanup)
    }
}
impl LocalCleanupOutput<()> for Result<(), crate::AktorFailure> {
    fn into_owner_cleanup(self) -> Result<(), OwnerError<()>> {
        self.map_err(OwnerError::CleanupFailure)
    }
}

pub trait LocalSetupOutput<S, C> {
    fn into_owner_setup(self) -> Result<S, OwnerError<C>>;
}
impl<S, C> LocalSetupOutput<S, C> for Result<S, AktorError<C>> {
    fn into_owner_setup(self) -> Result<S, OwnerError<C>> {
        self.map_err(OwnerError::Setup)
    }
}
impl<S, C, E> LocalSetupOutput<S, C>
    for (
        Result<S, (E, crate::AktorFailure)>,
        crate::setup::ready::Sender<Result<(), crate::AktorStartError<E>>>,
    )
{
    fn into_owner_setup(self) -> Result<S, OwnerError<C>> {
        let (result, ready) = self;

        match result {
            Ok(state) => {
                ready.send(Ok(()));
                Ok(state)
            }
            Err((data, error)) => {
                let diagnostic = AktorError::new(error.diagnostics.clone(), ());
                let primary = error.primary;
                ready.send(Err(crate::AktorStartError {
                    init: crate::AktorInitOutcome::Failed(data),
                    cause: crate::AktorStartCause::Init(error),
                    report: None,
                }));
                Err(OwnerError::SetupDiagnostic(diagnostic, primary))
            }
        }
    }
}
