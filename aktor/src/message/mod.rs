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
    #[er(format = "start the actor group listener before spawning actors")]
    NotStarted,
    #[er(format = "actor capacity is outside the supported range")]
    InvalidCapacity,
    #[er(format = "actor closed")]
    Closed,
}

pub type ActorResult<T> = Result<T, ActorError>;

#[cfg_attr(
    feature = "wasm_browser_workers",
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, Er, PartialEq, Eq)]
pub enum CallError {
    #[er(format = "actor did not admit the request")]
    NotAdmitted,
    #[er(format = "actor discarded the request before execution")]
    Discarded,
    #[er(format = "actor failed after the request started; the outcome is unknown")]
    OutcomeUnknown,
}

/// Failed immediate submission preserves the request.
#[derive(Er)]
pub enum TrySendError<R> {
    #[er(format = "actor mailbox is full")]
    Full(R),
    #[er(format = "actor closed")]
    Closed(R),
}

pub type LocalFuture<'a, O> = Pin<Box<dyn Future<Output = O> + 'a>>;

#[doc(hidden)]
pub fn boxed<'a, F: Future + 'a>(future: F) -> LocalFuture<'a, F::Output> {
    Box::pin(future)
}
