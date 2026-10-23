use alloc::boxed::Box;
use core::{future::Future, pin::Pin};
use er::Er;

mod error;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
mod native;
#[doc(hidden)]
pub use error::consumed;
#[cfg(any(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    feature = "local"
))]
#[doc(hidden)]
pub use error::stopped;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub use native::*;

#[derive(Clone, Copy, Er, PartialEq, Eq)]
pub enum ActorError {
    NotStarted,
    InvalidCapacity,
    Closed,
}

pub type ActorResult<T> = Result<T, ActorError>;

#[cfg_attr(feature = "transport", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Copy, Er, PartialEq, Eq)]
pub enum CallError {
    NotAdmitted,
    Discarded,
    OutcomeUnknown,
}

pub type LocalFuture<'a, O> = Pin<Box<dyn Future<Output = O> + 'a>>;

#[doc(hidden)]
pub fn boxed<'a, F: Future + 'a>(future: F) -> LocalFuture<'a, F::Output> {
    Box::pin(future)
}
