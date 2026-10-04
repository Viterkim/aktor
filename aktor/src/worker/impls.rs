use super::*;

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
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    #[doc(hidden)]
    pub fn data_error(mut self, error: WorkerError) -> Self {
        let message = match &mut self.cause {
            WorkerCause::Setup(message) | WorkerCause::Cleanup(message) => message,
            _ => return self,
        };
        message.push_str("\nOptional lifecycle data: ");
        message.push_str(&error.to_string());
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
