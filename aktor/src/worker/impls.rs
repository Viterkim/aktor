use super::*;

impl Options {
    pub fn validate(&self) -> Result<(), (WorkerError, String)> {
        validate_limits(self)
            .er(|| WorkerError::new(AktorDeliveryOutcome::NotAdmitted, WorkerCause::Setup))?;
        Ok(())
    }
}

#[derive(Er)]
enum LimitsErr {
    Capacity(usize),
    ByteBudget(usize),
}
fn validate_limits(options: &Options) -> ErResult<(), LimitsErr> {
    if options.capacity.get() > tokio::sync::Semaphore::MAX_PERMITS {
        er_bail!(LimitsErr::capacity(options.capacity.get()));
    }

    if options.max_outstanding_bytes.get() > u32::MAX as usize
        || options.max_outstanding_bytes.get() > tokio::sync::Semaphore::MAX_PERMITS
    {
        er_bail!(LimitsErr::byte_budget(options.max_outstanding_bytes.get()));
    }

    Ok(())
}
impl<T> WorkerError<T> {
    #[doc(hidden)]
    pub fn new(outcome: AktorDeliveryOutcome, cause: WorkerCause) -> Self {
        Self {
            outcome,
            cause,
            data: None,
            failure_id: None,
        }
    }
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
impl WireFailure {
    #[track_caller]
    pub(crate) fn new(outcome: AktorDeliveryOutcome, cause: WorkerCause) -> Self {
        let (error, report) = er_new!(WorkerError::new(outcome, cause)).er_split();
        Self { error, report }
    }

    #[track_caller]
    pub(crate) fn setup(outcome: AktorDeliveryOutcome, source: impl Into<String>) -> Self {
        let (error, report) = crate::report::with_report(
            WorkerError::new(outcome, WorkerCause::Setup),
            source.into(),
        );
        Self { error, report }
    }

    #[track_caller]
    pub(super) fn cleanup(outcome: AktorDeliveryOutcome, source: impl Into<String>) -> Self {
        let (error, report) = crate::report::with_report(
            WorkerError::new(outcome, WorkerCause::Cleanup),
            source.into(),
        );
        Self { error, report }
    }

    pub(crate) fn without_data<T>(self) -> (WorkerError<T>, String) {
        let mut error = WorkerError::new(self.error.outcome, self.error.cause);
        error.failure_id = self.error.failure_id;
        (error, self.report)
    }

    pub(super) fn with_outcome(&self, outcome: AktorDeliveryOutcome) -> Self {
        if self.error.outcome == outcome {
            return self.clone();
        }

        let mut error = self.error.clone();
        error.outcome = outcome;
        let report = er_new!(error.clone(), self.clone().without_data::<()>()).er_report();
        Self { error, report }
    }
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
impl From<(WorkerError, String)> for WireFailure {
    fn from((error, report): (WorkerError, String)) -> Self {
        let mut wire = WorkerError::new(error.outcome, error.cause);
        wire.failure_id = error.failure_id;
        Self {
            error: wire,
            report,
        }
    }
}
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
impl From<WireFailure> for (WorkerError, String) {
    fn from(failure: WireFailure) -> Self {
        failure.without_data()
    }
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
impl From<ErBranch<WorkerError>> for WireFailure {
    fn from(branch: ErBranch<WorkerError>) -> Self {
        crate::report::unlocated(branch).into()
    }
}
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
impl From<ErTree<WorkerError>> for WireFailure {
    fn from(tree: ErTree<WorkerError>) -> Self {
        tree.er_split().into()
    }
}
