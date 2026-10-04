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

#[cfg(feature = "tokio")]
pub use crate::message::{LatestResults as Results, LatestSender as Sender};
