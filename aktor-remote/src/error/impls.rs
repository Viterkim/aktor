use super::*;
use aktor::worker::{WorkerCause, WorkerError, WorkerFailure};
use std::{fmt, io};

impl Failure {
    pub fn new(outcome: CallError, cause: Cause, diagnostics: impl Into<String>) -> Self {
        Self {
            outcome,
            cause,
            diagnostics: diagnostics.into(),
        }
    }

    pub fn error(&self) -> Error {
        Error {
            outcome: self.outcome,
            cause: self.cause,
        }
    }
}
impl From<Failure> for (Error, String) {
    fn from(failure: Failure) -> Self {
        (failure.error(), failure.diagnostics)
    }
}
impl From<io::Error> for Failure {
    fn from(error: io::Error) -> Self {
        Self::new(CallError::NotAdmitted, Cause::Connection, error.to_string())
    }
}
impl From<WorkerFailure> for Failure {
    fn from(error: WorkerFailure) -> Self {
        let cause = match &error.cause {
            WorkerCause::Codec(_) => Cause::Codec,
            WorkerCause::Closed => Cause::Closed,
            _ => Cause::Protocol,
        };

        Self::new(error.outcome, cause, format!("{error:#}"))
    }
}
impl From<WorkerError> for Failure {
    fn from(error: WorkerError) -> Self {
        Self::from(WorkerFailure::from(error))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "remote call {:?}: {:?}", self.outcome, self.cause)
    }
}
impl std::error::Error for Error {}
