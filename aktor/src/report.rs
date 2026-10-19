use crate::{
    AktorDiagnostic, AktorError, AktorFailure, AktorGroupError, AktorRunError, ShutdownReport,
};
use alloc::{boxed::Box, string::String};
use core::fmt;

impl AktorGroupError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            diagnostics: None,
        }
    }

    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostics {
            Some(diagnostics) => f.write_str(&diagnostics.report),
            None => f.write_str(&self.message),
        }
    }
}
impl From<AktorGroupError> for AktorError {
    fn from(error: AktorGroupError) -> Self {
        let diagnostics = match error.diagnostics {
            Some(diagnostics) => diagnostics.report,
            None => error.message,
        };

        Self::new(diagnostics, ())
    }
}

impl From<AktorError> for AktorFailure {
    fn from(error: AktorError) -> Self {
        Self {
            diagnostics: error.diagnostics,
            primary: false,
        }
    }
}

impl From<ShutdownReport> for AktorRunError {
    fn from(report: ShutdownReport) -> Self {
        let diagnostics =
            AktorDiagnostic::new("actor group shutdown failed", alloc::format!("{report}"));

        Self {
            report: Box::new(report),
            diagnostics,
        }
    }
}

impl AktorRunError {
    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.diagnostics.report)
    }
}
