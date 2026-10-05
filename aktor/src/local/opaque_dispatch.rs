use super::*;
use crate::dispatch::{CallOutput, Read, ReadOpaque, Write, WriteOpaque};
use core::ops::AsyncFnOnce;

impl<'a, S: 'static, const N: usize, E, I: 'static, Role, Clock> ReadOpaque<S, I, Role>
    for &'a Handle<S, N, E, Role, Clock>
{
    type Output<Fut: Future>
        = Request<'a, S, N, E, <Fut::Output as CallOutput>::Output, Role, Clock>
    where
        Fut::Output: CallOutput;

    fn dispatch<F, Fut, O>(
        self,
        operation: Operation,
        function: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Output<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        let _ = factory;
        Read::dispatch(self, operation, function, input)
    }
}

impl<'a, S: 'static, const N: usize, E, I: 'static, Role, Clock> WriteOpaque<S, I, Role>
    for &'a Handle<S, N, E, Role, Clock>
{
    type Output<Fut: Future>
        = Request<'a, S, N, E, <Fut::Output as CallOutput>::Output, Role, Clock>
    where
        Fut::Output: CallOutput;

    fn dispatch<F, Fut, O>(
        self,
        operation: Operation,
        function: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Output<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        let _ = factory;
        Write::dispatch(self, operation, function, input)
    }
}
