#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]
#![doc = include_str!("../docs/functions.md")]

extern crate alloc;
extern crate self as aktor;

mod error;
mod lifecycle;
pub use error::{AktorCleanupError, AktorError, AktorSetupError};
pub use lifecycle::{ActorArgs, ActorFailure, ActorOutcome, ShutdownReport};

#[cfg(any(
    feature = "tokio",
    feature = "embassy",
    feature = "wasm_browser_workers"
))]
pub mod timeout;
#[cfg(any(
    feature = "tokio",
    feature = "embassy",
    feature = "wasm_browser_workers"
))]
pub use timeout::{AktorTimeoutError, Timeout};

#[cfg(any(feature = "tokio", feature = "wasm_browser_workers"))]
pub mod group;
#[cfg(any(feature = "tokio", feature = "wasm_browser_workers"))]
pub use group::{AktorGroup, GroupCompletion, KillSwitch};

pub mod call;
#[cfg(feature = "embassy")]
pub mod embassy;
pub mod latest;
#[cfg(feature = "tokio")]
pub mod listener;
pub mod message;
pub mod operation;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
pub mod owner;
#[cfg(feature = "tokio")]
mod queue;
pub mod target;
#[cfg(feature = "wasm_browser_workers")]
pub mod worker;

#[cfg(not(feature = "wasm_browser_workers"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_register {
    ($($operation:tt)*) => {};
}

#[cfg(feature = "macros")]
#[doc(inline)]
pub use aktor_macros::aktor;
#[doc(inline)]
#[cfg(feature = "tokio")]
pub use listener::{FailurePolicy, SpawnArgs};
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[doc(inline)]
pub use owner::Aktor;
