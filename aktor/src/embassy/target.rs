use super::*;
use crate::target::{Read, Write};
use core::ops::AsyncFnOnce;

impl<'a, S: 'static, const N: usize, E, I: 'static, Role> Read<S, I, Role>
    for &'a Handle<S, N, E, Role>
{
    type Output<O: Send + 'static> = Request<'a, S, N, E, O, Role>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        Request::new(
            self,
            operation,
            async move |state: &mut S, input| function(state, input).await,
            input,
        )
    }
}
impl<'a, S: 'static, const N: usize, E, I: 'static, Role> Write<S, I, Role>
    for &'a Handle<S, N, E, Role>
{
    type Output<O: Send + 'static> = Request<'a, S, N, E, O, Role>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        Request::new(self, operation, function, input)
    }
}
