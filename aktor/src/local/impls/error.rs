use super::super::*;

impl<E> core::fmt::Display for OwnerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let (context, source): (&str, Option<&dyn core::fmt::Display>) = match self {
            Self::Setup(error) => ("actor setup failed", Some(&error.diagnostics)),
            Self::SetupDiagnostic(error, _) => ("actor setup failed", Some(&error.diagnostics)),
            Self::SetupPanic(error) => ("actor setup panicked", Some(&error.diagnostics)),
            Self::Cleanup(error) => ("actor cleanup failed", Some(&error.diagnostics)),
            Self::CleanupFailure(error) => ("actor cleanup failed", Some(&error.diagnostics)),
            Self::Runner(error) => ("actor runner failed", Some(&error.diagnostics)),
            Self::Cancelled => ("actor owner stopped before cleanup completed", None),
        };

        f.write_str(context)?;

        if f.alternate()
            && let Some(error) = source
        {
            write!(f, ": {error}")?;
        }

        Ok(())
    }
}
impl<E> core::fmt::Debug for OwnerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:#}")
    }
}
impl<E: 'static> core::error::Error for OwnerError<E> {}
impl<E> OwnerError<E> {
    /// Copy the diagnostic, leaving the original typed data available to observers.
    pub fn report(&self) -> OwnerError {
        match self {
            Self::Setup(error) => OwnerError::Setup(error.report()),
            Self::SetupDiagnostic(error, primary) => {
                OwnerError::SetupDiagnostic(error.clone(), *primary)
            }
            Self::SetupPanic(error) => OwnerError::SetupPanic(error.clone()),
            Self::Cleanup(error) => OwnerError::Cleanup(error.report()),
            Self::CleanupFailure(error) => OwnerError::CleanupFailure(error.clone()),
            Self::Runner(error) => OwnerError::Runner(error.clone()),
            Self::Cancelled => OwnerError::Cancelled,
        }
    }
}

impl<E> Clone for SharedOwnerError<E> {
    fn clone(&self) -> Self {
        Self {
            error: self.error.clone(),
        }
    }
}
impl<E> core::fmt::Display for SharedOwnerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&*self.error, f)
    }
}
impl<E> core::fmt::Debug for SharedOwnerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:#}")
    }
}
impl<E: 'static> core::error::Error for SharedOwnerError<E> {}
