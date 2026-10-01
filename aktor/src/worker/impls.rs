use super::*;

impl Options {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.capacity == 0
            || self.capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_payload_bytes == 0
            || self.max_payload_bytes > self.max_outstanding_bytes
            || self.max_outstanding_bytes > u32::MAX as usize
            || self.max_outstanding_bytes > tokio::sync::Semaphore::MAX_PERMITS
        {
            Err(WorkerError::new(
                CallError::NotAdmitted,
                WorkerCause::PayloadTooLarge,
            ))
        } else {
            Ok(())
        }
    }
}
impl Default for Options {
    fn default() -> Self {
        Self {
            build: String::new(),
            timeout_ms: 5000,
            capacity: 32,
            max_payload_bytes: 1024 * 1024,
            max_outstanding_bytes: 4 * 1024 * 1024,
        }
    }
}

impl WorkerError {
    pub fn new(outcome: CallError, cause: WorkerCause) -> Self {
        Self { outcome, cause }
    }
}
impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "worker call {:?}: {:?}",
            self.outcome, self.cause
        )
    }
}
impl std::error::Error for WorkerError {}
