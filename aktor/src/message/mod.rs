use alloc::boxed::Box;
use core::{future::Future, pin::Pin};

mod error;
#[cfg(feature = "tokio")]
mod native;
#[cfg(any(feature = "tokio", feature = "embassy"))]
#[doc(hidden)]
pub use error::{consumed, stopped};
#[cfg(feature = "tokio")]
pub use native::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorError {
    InvalidCapacity,
    Closed,
}

pub type ActorResult<T> = Result<T, ActorError>;

#[cfg_attr(feature = "worker", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallError {
    NotAdmitted,
    Discarded,
    OutcomeUnknown,
    Superseded,
}

/// Failed immediate submission preserves the request.
pub enum TrySendError<R> {
    Full(R),
    Closed(R),
}

pub type LocalFuture<'a, O> = Pin<Box<dyn Future<Output = O> + 'a>>;

#[doc(hidden)]
pub fn boxed<'a, F: Future + 'a>(future: F) -> LocalFuture<'a, F::Output> {
    Box::pin(future)
}
