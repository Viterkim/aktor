use super::super::*;
use core::fmt;
use std::panic::{self, AssertUnwindSafe};

impl<C> Drop for FailedCleanup<C> {
    fn drop(&mut self) {
        // Don't let this cache's drop replace the actor's panic.
        for error in self.errors.drain(..) {
            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(error))) {
                crate::listener::failure::dispose_secondary(payload);
            }
        }
    }
}

impl<C: fmt::Display> fmt::Display for CleanupErrors<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} actor cleanup failure(s)",
            self.errors.len() + self.diagnostics.len()
        )?;

        if f.alternate() {
            for error in &self.errors {
                write!(f, ": {error:#}")?;
            }

            for diagnostic in &self.diagnostics {
                write!(f, ": {}", diagnostic.diagnostics)?;
            }
        }

        Ok(())
    }
}
impl<C: fmt::Debug + fmt::Display> std::error::Error for CleanupErrors<C> {}

impl<C: fmt::Display> fmt::Display for CleanupError<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Returned(error) => write!(f, "actor cleanup failed: {error}"),
            Self::Unconfirmed => f.write_str("actor cleanup result is unconfirmed"),
        }
    }
}
impl<C: std::error::Error + 'static> std::error::Error for CleanupError<C> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Returned(error) => Some(&**error),
            Self::Unconfirmed => None,
        }
    }
}
