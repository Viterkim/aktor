pub mod thread;
pub use thread::*;

use crate::{AktorFailure, message::LocalFuture, operation::Operation};
use alloc::boxed::Box;
use core::ops::AsyncFnMut;

pub struct AktorClosure<F: ?Sized>(pub Box<F>);

pub type AktorEndFuture =
    core::pin::Pin<Box<dyn core::future::Future<Output = Result<(), AktorFailure>> + Send>>;

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

impl<S, F> From<F> for AktorClosure<dyn FnMut(&mut S, Operation) + Send>
where
    F: FnMut(&mut S, Operation) + Send + 'static,
{
    fn from(function: F) -> Self {
        Self(Box::new(function))
    }
}
impl<S, F> From<F> for AktorClosure<dyn FnMut(&mut S, Operation)>
where
    F: FnMut(&mut S, Operation) + 'static,
{
    fn from(function: F) -> Self {
        Self(Box::new(function))
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
