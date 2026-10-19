use super::*;
use core::fmt;

impl Options {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.capacity == 0 || self.capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(WorkerError::setup(
                CallError::NotAdmitted,
                "worker queue capacity is outside the supported range",
            ));
        }

        if self.max_outstanding_bytes == 0
            || self.max_outstanding_bytes > u32::MAX as usize
            || self.max_outstanding_bytes > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(WorkerError::setup(
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
        let message = match &self.cause {
            WorkerCause::Setup => Some("worker setup failed"),
            WorkerCause::Cleanup => Some("worker cleanup failed"),
            WorkerCause::Operations { .. } => Some("worker operations do not match"),
            _ => None,
        };

        if let Some(message) = message {
            write!(f, "worker call {:?}: {message}", self.outcome)
        } else {
            write!(f, "worker call {:?}: {:?}", self.outcome, self.cause)
        }
    }

    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostics {
            Some(diagnostics) => f.write_str(&diagnostics.report),
            None => write!(f, "worker call {:?}: {:?}", self.outcome, self.cause),
        }
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    #[doc(hidden)]
    pub fn data_error(mut self, error: WorkerError) -> Self {
        if let Some(diagnostics) = &mut self.diagnostics {
            diagnostics.report.push_str("\nOptional lifecycle data: ");
            diagnostics.report.push_str(&alloc::format!("{error:#}"));
        }
        self
    }

    pub fn setup(outcome: CallError, report: impl Into<String>) -> Self {
        Self {
            outcome,
            cause: WorkerCause::Setup,
            diagnostics: Some(crate::AktorDiagnostic::new("worker setup failed", report)),
            data: None,
        }
    }

    pub fn cleanup(outcome: CallError, report: impl Into<String>) -> Self {
        Self {
            outcome,
            cause: WorkerCause::Cleanup,
            diagnostics: Some(crate::AktorDiagnostic::new("worker cleanup failed", report)),
            data: None,
        }
    }

    pub fn new(outcome: CallError, cause: WorkerCause) -> Self {
        let diagnostics = match &cause {
            WorkerCause::Operations { .. } => Some(crate::AktorDiagnostic::new(
                "worker operations do not match",
                alloc::format!("worker call {outcome:?}: {cause:?}"),
            )),
            _ => None,
        };

        Self {
            outcome,
            cause,
            diagnostics,
            data: None,
        }
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    pub fn without_data<U>(self) -> WorkerError<U> {
        WorkerError {
            outcome: self.outcome,
            cause: self.cause,
            diagnostics: self.diagnostics,
            data: None,
        }
    }
}
