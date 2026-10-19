use super::super::*;
use crate::AktorError;

impl<E> OwnerFailure<E> {
    fn summary(&self) -> (&str, Option<&str>) {
        match self {
            Self::Setup(err) => ("actor setup failed", Some(&err.diagnostics)),
            Self::SetupDiagnostic(err, _) => ("actor setup failed", Some(&err.diagnostics)),
            Self::SetupPanic(err) => ("actor setup panicked", Some(&err.diagnostics)),
            Self::Cleanup(err) => ("actor cleanup failed", Some(&err.diagnostics)),
            Self::CleanupFailure(err) => ("actor cleanup failed", Some(&err.diagnostics)),
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
    pub fn without_data(&self) -> OwnerFailure {
        match self {
            Self::Setup(error) => {
                OwnerFailure::Setup(AktorError::new(error.diagnostics.clone(), ()))
            }
            Self::SetupDiagnostic(error, primary) => {
                OwnerFailure::SetupDiagnostic(error.clone(), *primary)
            }
            Self::SetupPanic(error) => OwnerFailure::SetupPanic(error.clone()),
            Self::Cleanup(error) => {
                OwnerFailure::Cleanup(AktorError::new(error.diagnostics.clone(), ()))
            }
            Self::CleanupFailure(error) => OwnerFailure::CleanupFailure(error.clone()),
            Self::Runner(error) => OwnerFailure::Runner(error.clone()),
            Self::Cancelled => OwnerFailure::Cancelled,
        }
    }
}

impl<E> Clone for SharedOwnerError<E> {
    fn clone(&self) -> Self {
        Self {
            error: self.error.clone(),
            diagnostics: self.diagnostics.clone(),
        }
    }
}
impl<E> SharedOwnerError<E> {
    #[doc(hidden)]
    pub fn fmt_display(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.error)
    }

    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.diagnostics.report)
    }
}

impl<E> From<OwnerFailure<E>> for OwnerError<E> {
    fn from(error: OwnerFailure<E>) -> Self {
        let diagnostics =
            crate::AktorDiagnostic::new(alloc::format!("{error}"), alloc::format!("{error:#}"));

        Self { error, diagnostics }
    }
}
impl<E> OwnerError<E> {
    #[doc(hidden)]
    pub fn fmt_details(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.diagnostics.report)
    }
}
impl<E> SharedOwnerError<E> {
    pub fn from_failure(error: Rc<OwnerFailure<E>>, secondary: &[AktorError]) -> Self {
        let mut report = alloc::format!("{error:#}");

        for failure in secondary {
            report.push('\n');
            report.push_str(&failure.diagnostics);
        }

        let diagnostics = crate::AktorDiagnostic::new(alloc::format!("{error}"), report);

        Self { error, diagnostics }
    }

    pub fn without_data(self) -> OwnerError {
        OwnerError {
            error: self.error.without_data(),
            diagnostics: self.diagnostics,
        }
    }
}
