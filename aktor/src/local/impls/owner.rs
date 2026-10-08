use super::super::*;
use super::runner;

impl<S, const N: usize, E> Owner<S, N, E> {
    pub fn completion(&self) -> Completion<E> {
        Completion {
            inner: self.inner.completion.clone(),
        }
    }

    /// Run setup here on the owner's task.
    pub async fn run_with<Setup, SetupFuture, Cleanup, CleanupFuture>(
        self,
        setup: Setup,
        cleanup: Cleanup,
    ) -> Result<(), OwnerError>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError<E>>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<E>>>,
    {
        self.run_with_custom(setup, cleanup, runner::serve).await
    }

    /// Each operation completes before the next starts. Shutdown also awaits cleanup.
    pub async fn run<Cleanup, CleanupFuture>(
        self,
        state: S,
        cleanup: Cleanup,
    ) -> Result<(), OwnerError>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<E>>>,
    {
        self.run_custom(state, cleanup, runner::serve).await
    }

    pub(super) async fn dispose_hooks(&mut self) {
        let hooks = core::mem::take(&mut self.hooks);
        #[cfg(feature = "std")]
        {
            let report = |payload: &Box<dyn std::any::Any + Send>| {
                self.inner
                    .completion
                    .diagnostics
                    .borrow_mut()
                    .push(crate::AktorError::new(crate::panic::panic_message(payload)));
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
impl<S, const N: usize, E> Drop for Owner<S, N, E> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        {
            let report = |payload: &Box<dyn std::any::Any + Send>| {
                let error = crate::AktorError::new(crate::panic::panic_message(payload));
                self.inner.fail(&OwnerError::Runner(error.clone()));
                self.inner.completion.diagnostics.borrow_mut().push(error);
            };

            let hooks = core::mem::take(&mut self.hooks);
            let mut primary = crate::local::panic::discard_hooks(hooks, report);

            if let Some(payload) = &primary {
                report(payload);
            }

            let unfinished = self.inner.completion.result.borrow().is_none();

            if unfinished
                && let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.inner.finish(Err(Rc::new(OwnerError::Cancelled)));
                }))
            {
                report(&payload);

                if primary.is_none() {
                    primary = Some(payload);
                } else {
                    crate::panic::dispose_secondary(payload);
                }
            }

            if let Some(payload) = primary {
                if std::thread::panicking() {
                    crate::panic::dispose_secondary(payload);
                } else {
                    std::panic::resume_unwind(payload);
                }
            }
        }
        #[cfg(not(feature = "std"))]
        if self.inner.completion.result.borrow().is_none() {
            self.inner.finish(Err(Rc::new(OwnerError::Cancelled)));
        }
    }
}
