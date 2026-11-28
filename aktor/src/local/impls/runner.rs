use super::*;
use crate::{AktorCleanupError, report::ShutdownCut};
use core::{future::poll_fn, ops::AsyncFnOnce};

pub struct AktorRunner<'a, S, E = ()> {
    pub state: &'a mut S,
    hooks: &'a mut hooks::CallHooks<S>,
    inner: Rc<OwnerState<S, E>>,
    discarded: Rc<Cell<bool>>,
    waiting: &'a Cell<bool>,
}
impl<S, E> AktorRunner<'_, S, E> {
    pub async fn next(&mut self) -> Option<AktorCustomCall<'_, S, E>> {
        let _waiting = Waiting::new(self.waiting);
        let message = next(&self.inner).await?;

        Some(AktorCustomCall {
            inner: self.inner.clone(),
            discarded: self.discarded.clone(),
            completed: false,
            operation: message.operation,
            message,
            state: self.state,
            hooks: self.hooks,
        })
    }
}

struct Serving<'a, S>(Option<&'a Inner<S>>);

impl<S> Drop for Serving<'_, S> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        if std::thread::panicking() {
            return;
        }
        if let Some(inner) = self.0 {
            inner.cancelled();
        }
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
pub struct AktorCustomCall<'a, S, E = ()> {
    inner: Rc<OwnerState<S, E>>,
    discarded: Rc<Cell<bool>>,
    completed: bool,
    pub operation: Operation,
    message: Message<S>,
    state: &'a mut S,
    hooks: &'a mut hooks::CallHooks<S>,
}
impl<S, E> AktorCustomCall<'_, S, E> {
    pub async fn run(mut self) {
        let running = self.message.job.run(self.state, self.hooks, self.operation);

        #[cfg(feature = "std")]
        self.inner.capture(|| self.operation.phase(), running).await;

        #[cfg(not(feature = "std"))]
        {
            let _inner = &self.inner;
            running.await;
        }

        self.completed = true;
    }
}
impl<S, E> Drop for AktorCustomCall<'_, S, E> {
    fn drop(&mut self) {
        if !self.completed {
            self.discarded.set(true);
            self.inner.closed.notify();
        }
    }
}

pub async fn serve<S, E>(mut runner: AktorRunner<'_, S, E>) -> Result<(), (RunnerError, String)> {
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

pub async fn serve_on<Clock: clock::AktorGroupClock, S, E>(
    runner: AktorRunner<'_, S, E>,
) -> Result<(), (RunnerError, String)> {
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
pub async fn serve_browser<S, E>(
    mut runner: AktorRunner<'_, S, E>,
) -> Result<(), (RunnerError, String)> {
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

            // An empty mailbox says nothing here. A caller awaiting each reply refills it from
            // a microtask, and the page never gets a turn unless a timer gives it one.
            let _waiting = Waiting::new(waiting);
            crate::timeout::browser_sleep(core::time::Duration::ZERO).await;
        }
    }
}

async fn next<S, E>(inner: &OwnerState<S, E>) -> Option<Message<S>> {
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

fn try_next<S, E>(inner: &OwnerState<S, E>) -> Poll<Option<Message<S>>> {
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

impl<S, E> Owner<S, E> {
    pub async fn run_with_custom<Setup, SetupFuture, Cleanup, CleanupFuture, Runner>(
        self,
        setup: Setup,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), (OwnerError, String)>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, (E, String)>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), (E, String)>>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, E>) -> Result<(), (RunnerError, String)>,
    {
        let running = self.run_prepared(setup, cleanup, runner);
        #[cfg(feature = "std")]
        return crate::local::panic::original(running).await;
        #[cfg(not(feature = "std"))]
        running.await
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
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, E>) -> Result<(), (RunnerError, String)>,
    {
        // Shutdown giving up on this actor cancels its calls, a `start` still running stops there too.
        let inner = self.inner.clone();
        let changed = inner.closed.listen();
        let starting = async {
            let mut starting = core::pin::pin!(async {
                inner.init_evidence(crate::AktorInitEvidence::Entered);
                setup().await.into_owner_setup()
            });

            poll_fn(|cx| {
                changed.register(cx);

                if inner.cancelled.get() {
                    return Poll::Ready(Err(OwnerError::Cancelled.observed()));
                }

                starting.as_mut().poll(cx)
            })
            .await
        };

        #[cfg(feature = "std")]
        let (initialized, mut primary) = match self
            .inner
            .capture_result(|| crate::AktorPhase::Start, Box::pin(starting))
            .await
        {
            Ok(result) => (result, None),
            Err(crate::local::panic::Reported(payload, id)) => {
                self.inner.lost(id);
                let error = (
                    Rc::new(OwnerError::SetupPanic),
                    crate::local::panic::panic_report(&payload),
                );

                (Err(error), Some(payload))
            }
        };
        #[cfg(not(feature = "std"))]
        let initialized = starting.await;
        #[cfg(feature = "std")]
        let setup_panicked = primary.is_some();

        match initialized {
            Ok(state) => self.run_state(state, cleanup, runner).await,
            Err(error) => {
                self.inner.lost(self.inner.fail(&error.0, &error.1));
                #[cfg(feature = "std")]
                {
                    let inner = self.inner.clone();

                    if let Err(error) = inner
                        .settle(crate::AktorPhase::Drop, self.dispose_hooks(), &mut primary)
                        .await
                    {
                        inner
                            .completion
                            .diagnostics
                            .borrow_mut()
                            .push(error.er_report());
                    }

                    if let Err(error) = inner
                        .settle(
                            crate::AktorPhase::Drop,
                            async { drop(cleanup) },
                            &mut primary,
                        )
                        .await
                    {
                        inner
                            .completion
                            .diagnostics
                            .borrow_mut()
                            .push(error.er_report());
                    }

                    if let Err(error) = inner
                        .settle(
                            crate::AktorPhase::Drop,
                            async { drop(runner) },
                            &mut primary,
                        )
                        .await
                    {
                        inner
                            .completion
                            .diagnostics
                            .borrow_mut()
                            .push(error.er_report());
                    }

                    if !setup_panicked && let Some(payload) = primary.take() {
                        self.inner.dispose_panic(payload);
                    }
                }
                #[cfg(not(feature = "std"))]
                {
                    self.dispose_hooks().await;
                    drop((cleanup, runner));
                }

                if matches!(&*error.0, OwnerError::Cancelled) {
                    self.inner.completion.cut_call.set(true);
                } else if !matches!(&*error.0, OwnerError::SetupPanic) {
                    self.inner.completion.cleanup_completed.set(true);
                }
                let result = Err(error.clone());

                #[cfg(feature = "std")]
                {
                    if let Err(error) = self
                        .inner
                        .settle(
                            crate::AktorPhase::Drop,
                            async {
                                self.inner.finish(result.clone());
                            },
                            &mut primary,
                        )
                        .await
                    {
                        self.inner
                            .completion
                            .diagnostics
                            .borrow_mut()
                            .push(error.er_report());
                    }

                    if let Some(payload) = primary {
                        self.inner.resume(payload);
                    }
                }
                #[cfg(not(feature = "std"))]
                self.inner.finish(result.clone());
                let (error, report) = SharedOwnerError::from_failure(
                    error,
                    &self.inner.completion.diagnostics.borrow(),
                );
                Err((error.without_data(), report))
            }
        }
    }

    pub async fn run_custom<Cleanup, CleanupFuture, Runner>(
        self,
        state: S,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), (OwnerError, String)>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future,
        CleanupFuture::Output: LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, E>) -> Result<(), (RunnerError, String)>,
    {
        let running = self.run_state(state, cleanup, runner);
        #[cfg(feature = "std")]
        return crate::local::panic::original(running).await;
        #[cfg(not(feature = "std"))]
        running.await
    }

    async fn run_state<Cleanup, CleanupFuture, Runner>(
        mut self,
        mut state: S,
        cleanup: Cleanup,
        runner: Runner,
    ) -> Result<(), (OwnerError, String)>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future,
        CleanupFuture::Output: LocalCleanupOutput<E>,
        Runner: for<'a> AsyncFnOnce(AktorRunner<'a, S, E>) -> Result<(), (RunnerError, String)>,
    {
        self.inner.init_evidence(crate::AktorInitEvidence::Ready);
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
            let mut guard = Serving(Some(&inner));

            let result = poll_fn(|cx| {
                changed.register(cx);

                if inner.cancelled.get() {
                    // Waiting also covers the yield between batches, admitted calls can still be queued.
                    let running = polled && !waiting.get();

                    if running || inner.has_work() {
                        inner.completion.cut_call.set(true);
                        let cut = er_new!(ShutdownCut).er_report();
                        inner.lost(inner.record(crate::AktorPhase::Runner, cut.clone()));
                        inner.completion.diagnostics.borrow_mut().push(cut);
                    }

                    return Poll::Ready(Ok(()));
                }

                polled = true;
                let result = serving.as_mut().poll(cx);

                if discarded.get() {
                    Poll::Ready(Err(er_new!(
                        OwnerError::<()>::Runner,
                        RunnerError::DiscardedCall
                    )))
                } else {
                    result.map(|result| result.er(|| OwnerError::<()>::Runner))
                }
            })
            .await;
            guard.0 = None;
            result
        };

        #[cfg(feature = "std")]
        let mut primary = None;

        #[cfg(feature = "std")]
        let served = self
            .inner
            .settle(crate::AktorPhase::Runner, serving, &mut primary)
            .await;
        #[cfg(feature = "std")]
        let served = match served {
            Ok(result) => result,
            Err(error) => Err(crate::report::unlocated(
                ErBranch::from(OwnerError::<()>::Runner).er_add(error),
            )),
        };

        #[cfg(not(feature = "std"))]
        let served = serving.await;

        let result = served.and_then(|()| {
            let undrained = self.inner.open.get()
                || !self.inner.queue.borrow().is_empty()
                || !self.inner.services.borrow().is_empty();

            // Cancelled calls are dropped on purpose, only a runner that quit early failed.
            if undrained && !self.inner.cancelled.get() {
                Err(er_new!(
                    OwnerError::<()>::Runner,
                    RunnerError::StoppedBeforeDraining
                ))
            } else {
                Ok(())
            }
        });
        let mut result = match result {
            Ok(()) => Ok(()),
            Err(error) => Err((Rc::new(OwnerError::Runner), error.er_report())),
        };

        // A panicking runner was reported when it panicked.
        #[cfg(feature = "std")]
        let panicked = primary.is_some();
        #[cfg(not(feature = "std"))]
        let panicked = false;

        let runner_id = result
            .as_ref()
            .err()
            .filter(|_| !panicked)
            .and_then(|error| self.inner.fail(&error.0, &error.1));
        self.inner.lost(runner_id);

        let inner = self.inner.clone();
        let mut keep = |error: Observed<E>| {
            if result.is_err() {
                inner.completion.diagnostics.borrow_mut().push(error.1);
            } else {
                result = Err(error);
            }
        };

        #[cfg(feature = "std")]
        if let Err(error) = self
            .inner
            .settle(
                crate::AktorPhase::Drop,
                async { self.inner.close() },
                &mut primary,
            )
            .await
        {
            keep((Rc::new(OwnerError::Runner), error.er_report()));
        }
        #[cfg(not(feature = "std"))]
        self.inner.close();

        #[cfg(feature = "std")]
        if let Err(error) = self
            .inner
            .settle(
                crate::AktorPhase::Drop,
                async { self.inner.discard_queued() },
                &mut primary,
            )
            .await
        {
            keep((Rc::new(OwnerError::Runner), error.er_report()));
        }
        #[cfg(not(feature = "std"))]
        self.inner.discard_queued();

        let group = self
            .inner
            .group
            .borrow()
            .as_ref()
            .map(|(_, _, signal)| signal.clone());
        let ending = async {
            let end = cleanup(state);

            // In a group `end` gets its own grace, so one parked on an await can't hang shutdown.
            let output = match &group {
                Some(signal) => signal.end_within(end).await,
                None => Some(end.await),
            };

            let Some(output) = output else {
                self.inner.completion.ran_out.set(true);
                let failed = OwnerError::CleanupUnconfirmed.observed();
                return Err(failed);
            };

            let result = output.into_owner_cleanup();
            self.inner.completion.cleanup_completed.set(true);
            result
        };

        #[cfg(feature = "std")]
        let mut cleaned = match self
            .inner
            .settle(crate::AktorPhase::End, ending, &mut primary)
            .await
        {
            Ok(result) => result,
            Err(error) => Err((Rc::new(OwnerError::Runner), error.er_report())),
        };
        #[cfg(not(feature = "std"))]
        let mut cleaned = ending.await;

        if let Err((error, report)) = &mut cleaned
            && !matches!(**error, OwnerError::Runner)
        {
            let id = self.inner.fail(error, report);
            self.inner.remember(id);

            if let Some(OwnerError::CleanupFailure { id: stored }) = Rc::get_mut(error) {
                *stored = id;
            }
        }
        if let Err(error) = cleaned {
            keep(error);
        }

        #[cfg(feature = "std")]
        if let Err(error) = inner
            .settle(crate::AktorPhase::Drop, self.dispose_hooks(), &mut primary)
            .await
        {
            keep((Rc::new(OwnerError::Runner), error.er_report()));
        }
        #[cfg(not(feature = "std"))]
        self.dispose_hooks().await;

        #[cfg(feature = "std")]
        if let Err(error) = self
            .inner
            .settle(
                crate::AktorPhase::Drop,
                async {
                    self.inner.finish(result.clone());
                },
                &mut primary,
            )
            .await
        {
            self.inner
                .completion
                .diagnostics
                .borrow_mut()
                .push(error.er_report());
        }

        #[cfg(not(feature = "std"))]
        self.inner.finish(result.clone());

        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            self.inner.resume(payload);
        }

        match result {
            Ok(()) => Ok(()),
            Err(failure) => {
                let (error, report) = SharedOwnerError::from_failure(
                    failure,
                    &self.inner.completion.diagnostics.borrow(),
                );
                Err((error.without_data(), report))
            }
        }
    }
}

pub trait LocalCleanupOutput<E> {
    fn into_owner_cleanup(self) -> Result<(), (Rc<OwnerError<E>>, String)>;
}
impl<E> LocalCleanupOutput<E> for Result<(), (E, String)> {
    fn into_owner_cleanup(self) -> Result<(), Observed<E>> {
        match self {
            Ok(()) => Ok(()),
            Err((data, report)) => Err(OwnerError::Cleanup(data).with_report(report)),
        }
    }
}
impl LocalCleanupOutput<()> for Result<(), (AktorCleanupError, String)> {
    fn into_owner_cleanup(self) -> Result<(), Observed<()>> {
        match self {
            Ok(()) => Ok(()),
            Err((_error, report)) => {
                Err(OwnerError::CleanupFailure { id: None }.with_report(report))
            }
        }
    }
}

pub trait LocalSetupOutput<S, E> {
    fn into_owner_setup(self) -> Result<S, (Rc<OwnerError<E>>, String)>;
}
impl<S, E> LocalSetupOutput<S, E> for Result<S, (E, String)> {
    fn into_owner_setup(self) -> Result<S, Observed<E>> {
        match self {
            Ok(state) => Ok(state),
            Err((data, report)) => Err(OwnerError::Setup(data).with_report(report)),
        }
    }
}
impl<S, C, E> LocalSetupOutput<S, C>
    for (
        Result<S, (crate::setup::StartFailure<E>, crate::FailureId)>,
        crate::setup::ready::Startup<E>,
    )
{
    fn into_owner_setup(self) -> Result<S, Observed<C>> {
        let (result, ready) = self;

        match result {
            Ok(state) => {
                ready.send(Ok(()));
                Ok(state)
            }
            Err((failure, primary)) => {
                let report = failure.1.clone();
                ready.send(Err(failure));
                Err((Rc::new(OwnerError::SetupDiagnostic { id: primary }), report))
            }
        }
    }
}
