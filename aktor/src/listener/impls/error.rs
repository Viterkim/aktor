use super::super::*;
use core::fmt;

impl DedicatedJoinError {
    #[doc(hidden)]
    pub fn fmt_display(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = self.payload.lock();

        crate::panic::panic_tree(&payload).er_write_report(formatter)
    }
}
