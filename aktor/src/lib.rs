#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]
#![doc = include_str!("../docs/functions.md")]

extern crate alloc;
extern crate self as aktor;

#[cfg(feature = "embassy")]
pub mod embassy;
#[cfg(feature = "tokio")]
pub mod listener;
pub mod message;
pub mod operation;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
pub mod owner;
#[cfg(feature = "tokio")]
mod queue;
pub mod target;
#[cfg(feature = "worker")]
pub mod worker;

#[cfg(feature = "macros")]
#[doc(inline)]
pub use aktor_macros::aktor;
#[doc(inline)]
#[cfg(feature = "tokio")]
pub use listener::{FailurePolicy, SpawnArgs};
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[doc(inline)]
pub use owner::Aktor;
