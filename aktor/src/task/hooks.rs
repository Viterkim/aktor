use super::{AktorTaskFuture, AktorTaskState};
use crate::{AktorCleanupError, AktorClosure};
use core::future::Future;

pub trait AktorTaskEnd<S> {
    fn run(self: Box<Self>, state: S) -> AktorTaskFuture<'static, Result<(), AktorCleanupError>>;
}
impl<S, F, Fut> AktorTaskEnd<S> for F
where
    S: 'static,
    F: FnOnce(S) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), AktorCleanupError>> + Send + 'static,
{
    fn run(self: Box<Self>, state: S) -> AktorTaskFuture<'static, Result<(), AktorCleanupError>> {
        Box::pin(self(state))
    }
}
impl<S, F, Fut> From<F> for AktorClosure<dyn AktorTaskEnd<S> + Send>
where
    S: 'static,
    F: FnOnce(S) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), AktorCleanupError>> + Send + 'static,
{
    fn from(function: F) -> Self {
        Self(Box::new(function))
    }
}

pub trait AktorTaskInterval<S> {
    fn run(&mut self, state: AktorTaskState<S>) -> AktorTaskFuture<'static, ()>;
}
impl<S, F, Fut> AktorTaskInterval<S> for F
where
    S: 'static,
    F: FnMut(AktorTaskState<S>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn run(&mut self, state: AktorTaskState<S>) -> AktorTaskFuture<'static, ()> {
        Box::pin(self(state))
    }
}
impl<S, F, Fut> From<F> for AktorClosure<dyn AktorTaskInterval<S> + Send>
where
    S: 'static,
    F: FnMut(AktorTaskState<S>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn from(function: F) -> Self {
        Self(Box::new(function))
    }
}
