#![deny(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]
// Tokio on wasm without Bevy builds the actor plumbing, but no kind in that build runs a mailbox.
#![cfg_attr(
    all(target_family = "wasm", feature = "tokio", not(feature = "bevy")),
    allow(dead_code)
)]
#![doc = include_str!("../docs/examples.md")]
#![doc = include_str!("../docs/good-and-bad.md")]

extern crate alloc;
extern crate self as aktor;

mod impls;
#[cfg(all(
    feature = "std",
    any(
        feature = "local",
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    )
))]
mod panic;
mod report;
pub mod types;
pub use types::*;

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "wasm_browser_workers",
    feature = "browser_local"
))]
pub mod timeout;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "wasm_browser_workers",
    feature = "browser_local"
))]
pub use timeout::{AktorTimeout, AktorTimeoutError};

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "wasm_browser_workers",
    all(feature = "browser_local", target_family = "wasm")
))]
pub mod group;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "wasm_browser_workers",
    all(feature = "browser_local", target_family = "wasm")
))]
pub use group::{AktorGroup, AktorGroupCompletion, AktorGroupStopping, AktorKillSwitch, AktorRun};

pub mod call;
#[cfg(any(
    feature = "embassy_cross_core",
    all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    )
))]
pub mod cross_core;
pub mod data;
#[cfg(feature = "embassy")]
pub mod embassy;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
#[doc(hidden)]
pub mod executor;
#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_os = "unknown",
    target_feature = "atomics"
))]
pub mod threads;

pub mod latest;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
mod owner_loop;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub use owner_loop::{AktorLoop, AktorLoopEnd, AktorLoopHandle};
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "bevy"
))]
mod owner_tasks;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub use owner_tasks::aktor_spawn_local;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "bevy"
))]
pub use owner_tasks::{
    AktorJobError, AktorOwnerTask, AktorSpawnError, AktorStopGuard, AktorStopping, aktor_retire,
    aktor_spawn, aktor_stopping,
};
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
pub use message::{AktorDeliveryError, AktorDeliveryOutcome};
pub mod operation;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub mod owner;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "bevy"
))]
mod queue;
#[cfg(any(
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ),
    feature = "bevy"
))]
#[doc(hidden)]
pub mod serve;
#[cfg(any(
    feature = "local",
    all(
        any(feature = "tokio", feature = "std_thread"),
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
#[cfg(feature = "testing")]
pub mod testing;
#[cfg(any(
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
pub use setup::AktorDefinition;
#[cfg(any(
    feature = "local",
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ),
    all(
        any(feature = "browser_local", feature = "wasm_browser_workers"),
        target_family = "wasm",
        target_os = "unknown"
    )
))]
pub use setup::{AktorStart, aktor_start};
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
pub use task::{AktorTask, AktorTaskState};
#[cfg(feature = "transport")]
pub mod worker;

#[cfg(feature = "testing")]
#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_testing {
    ($($item:item)*) => { $($item)* };
}

#[cfg(not(feature = "testing"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_testing {
    ($($item:tt)*) => {};
}

#[cfg(not(feature = "transport"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_register {
    ($($operation:tt)*) => {};
}

#[cfg(not(all(
    target_family = "wasm",
    feature = "wasm_browser_workers",
    not(feature = "wasm_browser_worker_server")
)))]
#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_body {
    (body { $($body:tt)* } page { $($page:tt)* }) => { $($body)* };
    ($($body:tt)*) => { $($body)* };
}

#[cfg(all(
    target_family = "wasm",
    feature = "wasm_browser_workers",
    not(feature = "wasm_browser_worker_server")
))]
#[doc(hidden)]
#[macro_export]
macro_rules! __aktor_body {
    (body { $($body:tt)* } page { $($page:tt)* }) => { $($page)* };
    ($($body:tt)*) => {};
}

#[cfg(feature = "macros")]
#[doc(inline)]
pub use aktor_macros::AktorData;
#[cfg(feature = "macros")]
#[doc(inline)]
pub use aktor_macros::aktor;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
#[doc(inline)]
pub use owner::Aktor;

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use setup::{AktorWorkerNew, Optional, Required};
#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use worker::AktorCleanup;
#[cfg(all(
    feature = "wasm_browser_worker_server",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use worker::{AktorReady, AktorServe};
