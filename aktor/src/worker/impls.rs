use super::*;
use core::fmt;

impl Options {
    pub fn validate(&self) -> Result<(), (WorkerError, String)> {
        self.validate_limits().map_err(Into::into)
    }

    pub(super) fn validate_limits(&self) -> Result<(), WorkerFailure> {
        if self.capacity == 0 || self.capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(WorkerFailure::setup(
                CallError::NotAdmitted,
                "worker queue capacity is outside the supported range",
            ));
        }

        if self.max_outstanding_bytes == 0
            || self.max_outstanding_bytes > u32::MAX as usize
            || self.max_outstanding_bytes > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(WorkerFailure::setup(
                CallError::NotAdmitted,
                "worker byte budget is outside the supported range",
            ));
        }

        Ok(())
    }
}
impl Default for Options {
    fn default() -> Self {
        Self {
            build: String::new(),
            capacity: 32,
            max_outstanding_bytes: 4 * 1024 * 1024,
        }
    }
}

impl<T> WorkerError<T> {
    #[doc(hidden)]
    pub fn fmt_display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_error(f, self.outcome, &self.cause)
    }
}
impl WorkerError {
    #[doc(hidden)]
    pub fn new(outcome: CallError, cause: WorkerCause) -> Self {
        Self {
            outcome,
            cause,
            data: None,
        }
    }
}

pub(super) fn write_error(
    f: &mut fmt::Formatter<'_>,
    outcome: CallError,
    cause: &WorkerCause,
) -> fmt::Result {
    let message = match cause {
        WorkerCause::Setup => "worker setup failed",
        WorkerCause::Cleanup => "worker cleanup failed",
        WorkerCause::Operations { .. } => "worker operations do not match",
        cause => return write!(f, "worker call {outcome:?}: {cause:?}"),
    };

    write!(f, "worker call {outcome:?}: {message}")
}

impl<T> From<WorkerFailure<T>> for (WorkerError<T>, String) {
    fn from(failure: WorkerFailure<T>) -> Self {
        let error = WorkerError {
            outcome: failure.outcome,
            cause: failure.cause,
            data: failure.data,
        };
        let report = failure.diagnostics.unwrap_or_else(|| error.to_string());

        (error, report)
    }
}
impl<T> From<WorkerError> for WorkerFailure<T> {
    fn from(error: WorkerError) -> Self {
        Self {
            outcome: error.outcome,
            cause: error.cause,
            diagnostics: None,
            data: None,
        }
    }
}

impl<T> WorkerFailure<T> {
    #[doc(hidden)]
    pub fn fmt_display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_error(f, self.outcome, &self.cause)
    }

    #[cfg(all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    #[doc(hidden)]
    pub fn data_error(mut self, error: WorkerError) -> Self {
        if let Some(diagnostics) = &mut self.diagnostics {
            diagnostics.push_str("\nOptional lifecycle data: ");
            diagnostics.push_str(&alloc::format!("{error:#}"));
        }
        self
    }

    #[doc(hidden)]
    pub fn setup(outcome: CallError, report: impl Into<String>) -> Self {
        Self {
            outcome,
            cause: WorkerCause::Setup,
            diagnostics: Some(report.into()),
            data: None,
        }
    }

    #[doc(hidden)]
    pub fn cleanup(outcome: CallError, report: impl Into<String>) -> Self {
        Self {
            outcome,
            cause: WorkerCause::Cleanup,
            diagnostics: Some(report.into()),
            data: None,
        }
    }

    #[doc(hidden)]
    pub fn new(outcome: CallError, cause: WorkerCause) -> Self {
        let diagnostics = match &cause {
            WorkerCause::Operations { .. } => {
                Some(alloc::format!("worker call {outcome:?}: {cause:?}"))
            }
            _ => None,
        };

        Self {
            outcome,
            cause,
            diagnostics,
            data: None,
        }
    }

    #[cfg(all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    pub fn without_data<U>(self) -> WorkerFailure<U> {
        WorkerFailure {
            outcome: self.outcome,
            cause: self.cause,
            diagnostics: self.diagnostics,
            data: None,
        }
    }

    #[cfg(all(
        feature = "wasm_browser_workers",
        target_family = "wasm",
        target_os = "unknown"
    ))]
    pub(crate) fn report_without_data<U>(self) -> (WorkerError<U>, String) {
        self.without_data().into()
    }
}
