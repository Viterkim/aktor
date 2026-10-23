use super::*;
use aktor::worker::{WorkerCause, WorkerError};
use std::{fmt, io};

impl Error {
    pub fn new(outcome: CallError, cause: Cause, diagnostics: impl Into<String>) -> Self {
        Self {
            outcome,
            cause,
            diagnostics: diagnostics.into(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "remote call {:?}: {:?}", self.outcome, self.cause)?;

        if f.alternate() {
            write!(f, "\n{}", self.diagnostics)?;
        }

        Ok(())
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::new(CallError::NotAdmitted, Cause::Connection, error.to_string())
    }
}
impl From<WorkerError> for Error {
    fn from(error: WorkerError) -> Self {
        let cause = match &error.cause {
            WorkerCause::Codec(_) => Cause::Codec,
            WorkerCause::Closed => Cause::Closed,
            _ => Cause::Protocol,
        };

        Self::new(error.outcome, cause, format!("{error:#}"))
    }
}
