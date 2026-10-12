use super::*;
use core::fmt;

impl Options {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.capacity == 0 || self.capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Setup("worker queue capacity is outside the supported range".into()),
            ));
        }

        if self.max_outstanding_bytes == 0
            || self.max_outstanding_bytes > u32::MAX as usize
            || self.max_outstanding_bytes > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::Setup("worker byte budget is outside the supported range".into()),
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
            WorkerCause::Setup(_) => Some("worker setup failed"),
            WorkerCause::Cleanup(_) => Some("worker cleanup failed"),
            _ => None,
        };

        if let Some(message) = message {
            write!(f, "worker call {:?}: {message}", self.outcome)
        } else {
            self.fmt_details(f)
        }
    }

    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "worker call {:?}: {:?}", self.outcome, self.cause)
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    #[doc(hidden)]
    pub fn data_error(mut self, error: WorkerError) -> Self {
        let message = match &mut self.cause {
            WorkerCause::Setup(message) | WorkerCause::Cleanup(message) => message,
            _ => return self,
        };

        message.push_str("\nOptional lifecycle data: ");
        message.push_str(&alloc::format!("{error:#}"));
        self
    }

    pub fn new(outcome: CallError, cause: WorkerCause) -> Self {
        Self {
            outcome,
            cause,
            data: None,
        }
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    pub fn without_data<U>(self) -> WorkerError<U> {
        WorkerError {
            outcome: self.outcome,
            cause: self.cause,
            data: None,
        }
    }
}
