use super::*;

impl<R> fmt::Debug for TrySendError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("Full(..)"),
            Self::Closed(_) => f.write_str("Closed(..)"),
            Self::Rejected(_, error) => f.debug_tuple("Rejected").field(error).finish(),
        }
    }
}
impl<R> fmt::Display for TrySendError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("actor mailbox is full"),
            Self::Closed(_) => f.write_str("actor closed"),
            Self::Rejected(_, error) => error.fmt(f),
        }
    }
}
impl<R> std::error::Error for TrySendError<R> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(_, error) => Some(error),
            _ => None,
        }
    }
}
