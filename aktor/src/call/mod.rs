use crate::{latest::Factory, operation::Operation};
use core::{future::Future, pin::Pin};
use pin_project_lite::pin_project;

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser;
mod impls;
#[cfg(feature = "embassy")]
mod local;
#[cfg(feature = "tokio")]
mod native;

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
