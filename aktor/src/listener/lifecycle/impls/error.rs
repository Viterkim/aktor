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
        write!(f, "{} actor cleanup failure(s)", self.errors.len())?;

        for error in &self.errors {
            write!(f, ": {error}")?;
        }

        Ok(())
    }
}
impl<C: fmt::Debug + fmt::Display> std::error::Error for CleanupErrors<C> {}
