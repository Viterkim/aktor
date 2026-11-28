#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser;
#[cfg(any(
    feature = "embassy_cross_core",
    all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    )
))]
mod cross_core;
mod impls;
#[cfg(feature = "local")]
mod local;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
mod native;
mod request;
mod retry;
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
mod task;

#[doc(hidden)]
pub use request::{CastRequest, RetryRequest, SendRequest, TimeoutRequest};
pub use retry::Retried;

use crate::{latest::Factory, operation::Operation};
use core::future::{Future, IntoFuture};

/// The call returned by an #[aktor] function. Use `.into_future()` to pin or poll it yourself.
#[must_use = "await the call, send it, or make a latest session"]
pub struct Call<T, I, R, L = ()> {
    target: T,
    input: I,
    request: fn(T, I, Operation) -> R,
    factory: L,
    operation: Operation,
}
