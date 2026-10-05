use super::*;
use crate::{
    dispatch::{CallOutput, Queued, ReadOpaque, WriteOpaque},
    listener::Operation,
};
use core::{future::Future, ops::AsyncFnOnce};

impl<'a, S: 'static, E, C, I: Send + 'static, Role> ReadOpaque<S, I, Role>
    for &'a Aktor<S, E, C, Role>
{
    type Output<Fut: Future>
        = Queued<'a, S, I, <Fut::Output as CallOutput>::Output, Role>
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
        Queued::read(&self.handle, operation, function, input)
    }
}

impl<'a, S: 'static, E, C, I: Send + 'static, Role> WriteOpaque<S, I, Role>
    for &'a Aktor<S, E, C, Role>
{
    type Output<Fut: Future>
        = Queued<'a, S, I, <Fut::Output as CallOutput>::Output, Role>
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
        Queued::write(&self.handle, operation, function, input)
    }
}
