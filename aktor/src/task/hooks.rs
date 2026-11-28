use super::{AktorTaskFuture, AktorTaskState};
use crate::AktorClosure;
use core::future::Future;

#[doc(hidden)]
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
