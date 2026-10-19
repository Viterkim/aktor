use super::StartError;
use crate::{AktorDiagnostic, AktorFailure, listener::DedicatedStartError};
use core::fmt;

impl From<DedicatedStartError<AktorFailure>> for StartError {
    fn from(error: DedicatedStartError<AktorFailure>) -> Self {
        let diagnostics = match &error {
            DedicatedStartError::Init(failure) => Some(AktorDiagnostic::new(
                "actor initialization failed",
                failure.diagnostics.clone(),
            )),
            DedicatedStartError::Thread(cause) => Some(AktorDiagnostic::new(
                "actor thread could not start",
                format!("{cause:#}"),
            )),
            DedicatedStartError::Panicked { actor, cause } => Some(AktorDiagnostic::new(
                "actor initialization panicked",
                format!("actor {actor} initialization panicked:\n{cause}"),
            )),
            DedicatedStartError::NotStarted
            | DedicatedStartError::Closed
            | DedicatedStartError::NoRuntime
            | DedicatedStartError::InvalidCapacity => None,
        };

        Self { error, diagnostics }
    }
}
impl StartError {
    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostics {
            Some(diagnostics) => f.write_str(&diagnostics.report),
            None => fmt::Display::fmt(&self.error, f),
        }
    }
}
