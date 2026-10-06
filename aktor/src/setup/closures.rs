use crate::{AktorCleanupError, message::LocalFuture, operation::Operation};
use alloc::boxed::Box;
use core::ops::AsyncFnMut;

pub struct AktorClosure<F: ?Sized>(pub Box<F>);

pub trait AktorEnd<S, Data = ()> {
    fn run(&mut self, state: S) -> LocalFuture<'_, Result<(), AktorCleanupError<Data>>>;
}
impl<S: 'static, Data: 'static, F> AktorEnd<S, Data> for F
where
    F: AsyncFnMut(S) -> Result<(), AktorCleanupError<Data>>,
{
    fn run(&mut self, state: S) -> LocalFuture<'_, Result<(), AktorCleanupError<Data>>> {
        Box::pin(self(state))
    }
}

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
impl<S: 'static, Data: 'static, F> From<F> for AktorClosure<dyn AktorEnd<S, Data> + Send>
where
    F: AsyncFnMut(S) -> Result<(), AktorCleanupError<Data>> + Send + 'static,
{
    fn from(function: F) -> Self {
        Self(Box::new(function))
    }
}
impl<S: 'static, Data: 'static, F> From<F> for AktorClosure<dyn AktorEnd<S, Data>>
where
    F: AsyncFnMut(S) -> Result<(), AktorCleanupError<Data>> + 'static,
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
