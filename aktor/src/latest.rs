use crate::operation::Operation;
use core::ops::AsyncFnOnce;

#[doc(hidden)]
pub trait Factory<T, I> {
    type Sender;
    type Results;

    fn start(self, target: T, input: I, operation: Operation) -> (Self::Sender, Self::Results);
}

/// Update one ongoing operation.
pub trait SendLatest<I> {
    fn send(&self, input: I);
}

#[doc(hidden)]
pub trait Session<S, I, O, Role = ()> {
    type Sender: SendLatest<I>;
    type Results;

    fn session<F>(self, operation: Operation, function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static;
}

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
pub use crate::message::{LatestResults as Results, LatestSender as Sender};

#[doc(hidden)]
pub trait TypedSession<S, I, O, Lease, Role = ()> {
    type Sender<Fut>;
    type Results;

    fn session<F, Fut>(
        self,
        operation: Operation,
        function: F,
        factory: fn(Lease, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: core::future::Future<Output = (Lease, O)>;
}
