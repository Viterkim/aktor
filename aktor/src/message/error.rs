use super::*;
use core::fmt;

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidCapacity => "actor capacity is outside the supported range",
            Self::Closed => "actor closed",
        })
    }
}
impl core::error::Error for ActorError {}

impl fmt::Display for CallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAdmitted => formatter.write_str("actor did not admit the request"),
            Self::Discarded => formatter.write_str("actor discarded the request before execution"),
            Self::Superseded => formatter.write_str("newer queued work superseded the request"),
            Self::OutcomeUnknown => formatter
                .write_str("actor failed after the request started; the outcome is unknown"),
        }
    }
}
impl core::error::Error for CallError {}

impl<R> fmt::Debug for TrySendError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full(_) => "Full(..)",
            Self::Closed(_) => "Closed(..)",
        })
    }
}
impl<R> fmt::Display for TrySendError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full(_) => "actor mailbox is full",
            Self::Closed(_) => "actor closed",
        })
    }
}
impl<R> core::error::Error for TrySendError<R> {}

#[cfg(any(feature = "tokio", feature = "embassy"))]
pub fn stopped() -> ! {
    panic!("actor stopped before replying")
}

#[cfg(any(feature = "tokio", feature = "embassy"))]
pub fn consumed() -> ! {
    panic!("actor request output already consumed")
}
