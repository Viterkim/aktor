use super::super::*;

impl<E> core::fmt::Display for OwnerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let (context, source): (&str, Option<&dyn core::fmt::Display>) = match self {
            Self::Setup(error) => ("actor setup failed", Some(error)),
            Self::SetupPanic(error) => ("actor setup panicked", Some(error)),
            Self::Cleanup(error) => ("actor cleanup failed", Some(error)),
            Self::Runner(error) => ("actor runner failed", Some(error)),
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
impl<E: 'static> core::error::Error for OwnerError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Setup(error) | Self::Cleanup(error) => Some(error),
            Self::SetupPanic(error) | Self::Runner(error) => Some(error),
            Self::Cancelled => None,
        }
    }
}
impl<E> OwnerError<E> {
    /// Copy the diagnostic, leaving the original typed data available to observers.
    pub fn report(&self) -> OwnerError {
        match self {
            Self::Setup(error) => OwnerError::Setup(error.report()),
            Self::SetupPanic(error) => OwnerError::SetupPanic(error.clone()),
            Self::Cleanup(error) => OwnerError::Cleanup(error.report()),
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
impl<E: 'static> core::error::Error for SharedOwnerError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&*self.error)
    }
}
