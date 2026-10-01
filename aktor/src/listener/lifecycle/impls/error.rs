use super::super::*;
use core::fmt;

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
