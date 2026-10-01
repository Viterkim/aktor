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

impl<E: fmt::Display> fmt::Display for DedicatedStartError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRuntime => formatter.write_str("an owned actor needs a Tokio runtime"),
            Self::InvalidCapacity => {
                formatter.write_str("actor capacity is outside Tokio's supported range")
            }
            Self::Thread(error) => write!(formatter, "could not spawn actor thread: {error}"),
            Self::Init(error) => write!(formatter, "could not initialize actor: {error}"),
            Self::Panicked { actor, cause } => {
                write!(
                    formatter,
                    "actor {actor:?} panicked while initializing: {cause}"
                )
            }
        }
    }
}
impl<E: core::error::Error + 'static> core::error::Error for DedicatedStartError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Init(error) => Some(error),
            Self::Thread(error) => Some(error),
            Self::Panicked { cause, .. } => Some(cause),
            _ => None,
        }
    }
}
