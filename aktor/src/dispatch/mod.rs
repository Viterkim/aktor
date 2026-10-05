#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
use crate::{
    listener::Handle,
    message::{Request, call_async},
};
use crate::{message::LocalFuture, operation::Operation};
use alloc::boxed::Box;
use core::{future::Future, ops::AsyncFnOnce};

pub mod call;
pub mod impls;
pub use call::{CallOutput, Direct, OwnedState, ReadCall, ReadState, WriteCall, WriteState};
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub mod request;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub use request::Queued;
pub mod opaque;
pub use opaque::{ReadOpaque, WriteOpaque};

#[doc(hidden)]
pub struct Native;

#[doc(hidden)]
pub struct Remote;

/// A data transport used by generated operations with concrete signatures.
pub trait Transport<S, I, O, Role = ()> {
    type Request: Future<Output = O>;

    fn request(self, operation: Operation, input: I) -> Self::Request;
}

/// The local body and types needed when registering a concrete operation for transport.
pub trait Export {
    type State: 'static;
    type Input;
    type Output: 'static;
    type Role;

    fn run(self, state: &mut Self::State, input: Self::Input) -> LocalFuture<'_, Self::Output>;
}

/// Dispatch with the result type known before choosing the execution backend.
pub trait Read<S: 'static, I, O: 'static, Role = ()> {
    type Request: Future<Output = O>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static;
}

pub trait Write<S: 'static, I, O: 'static, Role = ()> {
    type Request: Future<Output = O>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static;
}
