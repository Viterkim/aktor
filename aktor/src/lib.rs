#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]
#![doc = include_str!("../docs/functions.md")]

extern crate alloc;
extern crate self as aktor;

mod impls;
pub mod types;
pub use types::*;

#[cfg(any(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    feature = "embassy",
    feature = "wasm_browser_workers",
    feature = "browser_local"
))]
pub mod timeout;
#[cfg(any(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    feature = "embassy",
    feature = "wasm_browser_workers",
    feature = "browser_local"
))]
pub use timeout::{AktorTimeoutError, Timeout};

#[cfg(any(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    feature = "wasm_browser_workers",
    all(feature = "browser_local", target_family = "wasm")
))]
pub mod group;
#[cfg(any(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    feature = "wasm_browser_workers",
    all(feature = "browser_local", target_family = "wasm")
))]
pub use group::{AktorGroup, GroupCompletion, KillSwitch};

pub mod call;
#[cfg(feature = "embassy_cross_core")]
pub mod cross_core;
#[cfg(feature = "embassy")]
pub mod embassy;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
#[doc(hidden)]
pub mod executor;
pub mod latest;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub mod listener;
#[cfg(feature = "local")]
pub mod local;
#[cfg(feature = "local")]
pub use local::{AktorCustomCall, AktorRunner};
pub mod dispatch;
pub mod message;
pub mod operation;
#[cfg(all(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    not(target_family = "wasm")
))]
pub mod owner;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
mod queue;
#[cfg(any(
    feature = "local",
    all(
        any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
pub mod setup;
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
pub mod task;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub use setup::AktorThreadStartError;
#[cfg(any(
    feature = "local",
    all(
        any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
pub use setup::{start, start_in};
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
pub use task::{AktorTask, AktorTaskState};
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
pub use aktor_macros::{aktor, aktor_setups};
#[doc(inline)]
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub use listener::{FailurePolicy, SpawnArgs};
#[cfg(all(
    any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ),
    not(target_family = "wasm")
))]
#[doc(inline)]
pub use owner::Aktor;

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use setup::{AktorWorkerOptions, AktorWorkerSetup};
