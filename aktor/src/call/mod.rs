use crate::{latest::Factory, operation::Operation};
use core::{future::Future, pin::Pin};
use pin_project_lite::pin_project;

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
mod request;
mod retry;
#[doc(hidden)]
pub use request::{CastRequest, RetryRequest, SendRequest, TimeoutRequest};
pub use retry::Retried;
#[cfg(feature = "local")]
mod local;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
mod native;
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
mod task;

pin_project! {
    /// The call returned by an #[aktor] function.
    #[must_use = "await the call, send it, or make a latest session"]
    pub struct Call<T, I, R, L = ()> {
        pending: Option<(T, I)>,
        #[pin]
        running: Option<R>,
        request: fn(T, I, Operation) -> R,
        latest: L,
        operation: Operation,
    }
}
