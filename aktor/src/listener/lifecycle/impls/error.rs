use super::super::*;
use core::fmt;

impl<E: fmt::Display> fmt::Display for LifecycleError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("actor stopped"),
            Self::AlreadyRunning => formatter.write_str("actor is already running"),
            Self::Failed(error) => write!(formatter, "actor lifecycle operation failed: {error}"),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for LifecycleError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }
}

impl<C: fmt::Display> fmt::Display for CleanupErrors<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} actor cleanup failure(s)", self.errors.len())?;

        for error in &self.errors {
            write!(f, ": {error}")?;
        }

        Ok(())
    }
}
impl<C: fmt::Debug + fmt::Display> std::error::Error for CleanupErrors<C> {}

impl<E: fmt::Display, C: fmt::Display> fmt::Display for ReplaceError<E, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("actor stopped"),
            Self::Cleanup(error) => write!(f, "replacement cleanup failed: {error}"),
            Self::Setup(error) => write!(f, "replacement setup failed: {error}"),
        }
    }
}
impl<E: std::error::Error + 'static, C: std::error::Error + 'static> std::error::Error
    for ReplaceError<E, C>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Setup(error) => Some(error),
            Self::Cleanup(error) => Some(error.as_ref()),
            Self::Closed => None,
        }
    }
}
