use super::super::*;
use crate::AktorError;

impl<E> OwnerError<E> {
    fn summary(&self) -> (&str, Option<&str>) {
        match self {
            Self::Setup(err) => ("actor setup failed", Some(&err.diagnostics)),
            Self::SetupDiagnostic(err, _) => ("actor setup failed", Some(&err.diagnostics)),
            Self::SetupPanic(err) => ("actor setup panicked", Some(&err.diagnostics)),
            Self::Cleanup(err) => ("actor cleanup failed", Some(&err.diagnostics)),
            Self::CleanupFailure(err) => ("actor cleanup failed", Some(&err.diagnostics)),
            Self::CleanupUnconfirmed => ("actor stopped before cleanup completed", None),
            Self::Runner(err) => ("actor runner failed", Some(&err.diagnostics)),
            Self::Cancelled => ("actor owner stopped before cleanup completed", None),
        }
    }

    #[doc(hidden)]
    pub fn fmt_display(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.summary().0)
    }

    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let (context, source) = self.summary();
        f.write_str(context)?;

        if let Some(err) = source
            && !err.is_empty()
        {
            write!(f, ": {err}")?;
        }

        Ok(())
    }

    /// Copy the diagnostic, leaving the original typed data available to observers.
    pub fn without_data(&self) -> OwnerError {
        match self {
            Self::Setup(error) => OwnerError::Setup(AktorError::new(error.diagnostics.clone(), ())),
            Self::SetupDiagnostic(error, primary) => {
                OwnerError::SetupDiagnostic(error.clone(), *primary)
            }
            Self::SetupPanic(error) => OwnerError::SetupPanic(error.clone()),
            Self::Cleanup(error) => {
                OwnerError::Cleanup(AktorError::new(error.diagnostics.clone(), ()))
            }
            Self::CleanupFailure(error) => OwnerError::CleanupFailure(error.clone()),
            Self::CleanupUnconfirmed => OwnerError::CleanupUnconfirmed,
            Self::Runner(error) => OwnerError::Runner(error.clone()),
            Self::Cancelled => OwnerError::Cancelled,
        }
    }
}

impl<E> Clone for SharedOwnerError<E> {
    fn clone(&self) -> Self {
        Self {
            cause: self.cause.clone(),
        }
    }
}
impl<E> From<OwnerError<E>> for (OwnerError<E>, String) {
    fn from(error: OwnerError<E>) -> Self {
        let report = alloc::format!("{error:#}");

        (error, report)
    }
}
impl<E> SharedOwnerError<E> {
    #[doc(hidden)]
    pub fn from_failure(error: Rc<OwnerError<E>>, secondary: &[AktorError]) -> (Self, String) {
        let mut report = alloc::format!("{error:#}");

        for failure in secondary {
            report.push('\n');
            report.push_str(&failure.diagnostics);
        }

        (Self { cause: error }, report)
    }

    pub fn without_data(self) -> OwnerError {
        self.cause.without_data()
    }
}
