use super::super::*;
use core::fmt;

impl fmt::Display for DedicatedJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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
impl core::error::Error for DedicatedJoinError {}
