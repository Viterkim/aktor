use super::super::*;
use core::fmt;

impl<E> DedicatedStartError<E> {
    /// The original startup failure beneath any attached join results.
    pub fn primary(&self) -> &Self {
        if let Self::Startup { cause, .. } = self {
            return cause.primary();
        }
        self
    }
}

impl DedicatedJoinError {
    #[doc(hidden)]
    pub fn fmt_display(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = self.payload.lock();

        if let Some(message) = payload.downcast_ref::<&str>() {
            formatter.write_str(message)
        } else if let Some(message) = payload.downcast_ref::<String>() {
            formatter.write_str(message)
        } else {
            formatter.write_str("actor thread panicked")
        }
    }
}
