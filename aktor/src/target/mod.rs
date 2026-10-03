#[cfg(feature = "tokio")]
use crate::{
    listener::Handle,
    message::{Request, call_async},
};
use crate::{message::LocalFuture, operation::Operation};
use alloc::boxed::Box;
use core::{future::Future, ops::AsyncFnOnce};

pub mod impls;

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
    type Output: Send + 'static;
    type Role;

    fn run(self, state: &mut Self::State, input: Self::Input) -> LocalFuture<'_, Self::Output>;
}

/// A shared local state reference or a handle to that state.
#[diagnostic::on_unimplemented(
    note = "Browser workers require concrete owned argument and output types. Generic or borrowed operations can run locally or through native and Embassy handles."
)]
pub trait Read<S: 'static, I, Role = ()> {
    type Output<O: Send + 'static>: Future<Output = O>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: Send + 'static;
}

/// An exclusive local state reference or a handle to that state.
#[diagnostic::on_unimplemented(
    note = "Browser workers require concrete owned argument and output types. Generic or borrowed operations can run locally or through native and Embassy handles."
)]
pub trait Write<S: 'static, I, Role = ()> {
    type Output<O: Send + 'static>: Future<Output = O>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: Send + 'static;
}
