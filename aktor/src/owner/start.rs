use super::StartError;
use crate::{AktorDiagnostic, AktorFailure, listener::DedicatedStartError};
use core::fmt;

impl From<DedicatedStartError<AktorFailure>> for StartError {
    fn from(error: DedicatedStartError<AktorFailure>) -> Self {
        let mut diagnostics = match error.primary() {
            DedicatedStartError::Init(failure) => Some(AktorDiagnostic::new(
                "actor initialization failed",
                failure.diagnostics.clone(),
            )),
            DedicatedStartError::ThreadNotCreated(cause) => Some(AktorDiagnostic::new(
                "actor thread could not start",
                format!("{cause:#}"),
            )),
            DedicatedStartError::Thread(cause) => Some(AktorDiagnostic::new(
                "actor runtime could not start",
                format!("{cause:#}"),
            )),
            DedicatedStartError::Panicked { actor, cause } => Some(AktorDiagnostic::new(
                "actor initialization panicked",
                format!("actor {actor} initialization panicked:\n{cause}"),
            )),
            DedicatedStartError::Startup { .. } => None,
            DedicatedStartError::Observer { cause, .. } => Some(AktorDiagnostic::new(
                "actor completion observer could not start",
                format!("{cause:#}"),
            )),
            DedicatedStartError::NotStarted
            | DedicatedStartError::Closed
            | DedicatedStartError::NoRuntime
            | DedicatedStartError::InvalidCapacity => None,
        };

        let secondary = match &error {
            DedicatedStartError::Startup {
                join: crate::listener::StartupJoin::Failed(cause),
                ..
            } => Some(format!("Startup join failed: {cause}")),
            DedicatedStartError::Observer {
                completion: Err(cause),
                ..
            } => Some(format!("Owner completion failed: {cause:#}")),
            _ => None,
        };

        if let Some(secondary) = secondary {
            match &mut diagnostics {
                Some(diagnostics) => {
                    diagnostics.report.push('\n');
                    diagnostics.report.push_str(&secondary);
                }
                None => diagnostics = Some(AktorDiagnostic::new("actor startup failed", secondary)),
            }
        }

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
