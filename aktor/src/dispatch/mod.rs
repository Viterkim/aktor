pub mod call;
pub mod impls;
pub mod opaque;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub mod request;

mod transport;

pub use call::{CallOutput, Direct, OwnedState, ReadCall, ReadState, WriteCall, WriteState};
pub use opaque::{ReadOpaque, WriteOpaque};
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub use request::Queued;
#[doc(hidden)]
pub use transport::{ReadDispatch, WriteDispatch};

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

#[doc(hidden)]
pub struct Native;

#[doc(hidden)]
pub struct Remote;

#[doc(hidden)]
pub struct DataCodec;

/// The codec of an `#[aktor(remote)]` function. A remote client only takes this one, so a plain
/// `#[aktor]` function on a client fails to compile instead of failing on the server.
#[doc(hidden)]
pub struct RemoteCodec<C>(core::marker::PhantomData<fn() -> C>);

/// A data transport used by generated operations with concrete signatures.
#[diagnostic::on_unimplemented(
    note = "Derive AktorData for the arguments and result of a worker call."
)]
pub trait Transport<S, I, O, Role = (), Codec = DataCodec> {
    type Request: Future;

    fn request(self, operation: Operation, input: I) -> Self::Request;
}

/// The local body and types needed when registering a concrete operation for transport.
#[doc(hidden)]
pub trait Export<Codec = DataCodec> {
    const REMOTE: bool = false;
    const NAME: &'static str = "";

    type State: 'static;
    type Input;
    type Output: 'static;
    type Role;

    #[cfg(not(all(
        target_family = "wasm",
        feature = "wasm_browser_workers",
        not(feature = "wasm_browser_worker_server")
    )))]
    /// The registry boxes the whole execution once, this future lives inside that box.
    fn run(
        self,
        state: &mut Self::State,
        input: Self::Input,
    ) -> impl Future<Output = Self::Output> + '_;
}

/// Dispatch with the result type known before choosing the execution backend.
pub trait Read<S: 'static, I, O: 'static, Role = ()> {
    type Request: Future;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static;
}

pub trait Write<S: 'static, I, O: 'static, Role = ()> {
    type Request: Future;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static;
}
