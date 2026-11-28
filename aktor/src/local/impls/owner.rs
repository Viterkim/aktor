use super::super::*;
use super::runner;

impl<S, E> Owner<S, E> {
    pub fn completion(&self) -> Completion<E> {
        Completion {
            inner: self.inner.completion.clone(),
            result: self.inner.result.clone(),
        }
    }

    /// Run setup here on the owner's task.
    pub async fn run_with<Setup, SetupFuture, Cleanup, CleanupFuture>(
        self,
        setup: Setup,
        cleanup: Cleanup,
    ) -> Result<(), (OwnerError, String)>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, (E, String)>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), (E, String)>>,
    {
        self.run_with_custom(setup, cleanup, runner::serve).await
    }

    /// Each operation completes before the next starts. Shutdown also awaits cleanup.
    pub async fn run<Cleanup, CleanupFuture>(
        self,
        state: S,
        cleanup: Cleanup,
    ) -> Result<(), (OwnerError, String)>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), (E, String)>>,
    {
        self.run_custom(state, cleanup, runner::serve).await
    }

    pub async fn dispose_hooks(&mut self) {
        let hooks = core::mem::take(&mut self.hooks);
        #[cfg(feature = "std")]
        {
            let report = |payload: &Box<dyn std::any::Any + Send>| {
                self.inner.save_panic(crate::AktorPhase::Drop, payload);
            };

            let primary = crate::local::panic::discard_hooks(hooks, report);

            if let Some(payload) = primary {
                std::panic::resume_unwind(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        drop(hooks);
    }
}
impl<S, E> Drop for Owner<S, E> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        {
            let report = |payload: &Box<dyn std::any::Any + Send>| {
                self.inner
                    .report_panic(crate::AktorPhase::Drop, payload, true);
            };

            let hooks = core::mem::take(&mut self.hooks);
            let mut primary = crate::local::panic::discard_hooks(hooks, report);

            if let Some(payload) = &primary {
                report(payload);
            }

            let unfinished = self.inner.completion.result.borrow().is_none();

            if unfinished
                && let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let failure = OwnerError::Cancelled.observed();
                    self.inner.cancelled();
                    self.inner.finish(Err(failure));
                }))
            {
                report(&payload);

                if primary.is_none() {
                    primary = Some(payload);
                } else {
                    self.inner.dispose_panic(payload);
                }
            }

            if let Some(payload) = primary {
                if std::thread::panicking() {
                    self.inner.dispose_panic(payload);
                } else {
                    self.inner.resume(payload);
                }
            }
        }
        #[cfg(not(feature = "std"))]
        if self.inner.completion.result.borrow().is_none() {
            let failure = OwnerError::Cancelled.observed();
            self.inner.cancelled();
            self.inner.finish(Err(failure));
        }
    }
}
