use alloc::string::String;
pub mod thread;
pub use thread::*;

use crate::{AktorCleanupError, message::LocalFuture, operation::Operation};
use alloc::boxed::Box;
use core::ops::AsyncFnMut;

pub struct AktorClosure<F: ?Sized>(pub(crate) Box<F>);

pub type AktorEndFuture = core::pin::Pin<
    Box<dyn core::future::Future<Output = Result<(), (AktorCleanupError, String)>> + Send>,
>;

#[doc(hidden)]
pub trait AktorIntervalLogic<S> {
    fn run<'a>(&'a mut self, state: &'a mut S) -> LocalFuture<'a, ()>;
}
impl<S, F> AktorIntervalLogic<S> for F
where
    F: for<'a> AsyncFnMut(&'a mut S),
{
    fn run<'a>(&'a mut self, state: &'a mut S) -> LocalFuture<'a, ()> {
        Box::pin(self(state))
    }
}

#[doc(hidden)]
pub trait EachHook<T: ?Sized> {
    fn boxed(self) -> Option<Box<T>>;
}

impl<T: ?Sized> EachHook<T> for () {
    fn boxed(self) -> Option<Box<T>> {
        None
    }
}

impl<S, F> EachHook<dyn FnMut(&mut S, Operation) + Send> for F
where
    F: FnMut(&mut S, Operation) + Send + 'static,
{
    fn boxed(self) -> Option<Box<dyn FnMut(&mut S, Operation) + Send>> {
        Some(Box::new(self))
    }
}

impl<S, F> EachHook<dyn FnMut(&mut S, Operation)> for F
where
    F: FnMut(&mut S, Operation) + 'static,
{
    fn boxed(self) -> Option<Box<dyn FnMut(&mut S, Operation)>> {
        Some(Box::new(self))
    }
}

impl<S: 'static, F> From<F> for AktorClosure<dyn AktorIntervalLogic<S> + Send>
where
    F: for<'a> AsyncFnMut(&'a mut S) + Send + 'static,
{
    fn from(function: F) -> Self {
        Self(Box::new(function))
    }
}
impl<S: 'static, F> From<F> for AktorClosure<dyn AktorIntervalLogic<S>>
where
    F: for<'a> AsyncFnMut(&'a mut S) + 'static,
{
    fn from(function: F) -> Self {
        Self(Box::new(function))
    }
}
