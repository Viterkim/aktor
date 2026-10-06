use super::*;
use crate::dispatch::{
    CallOutput, ReadCall, ReadOpaque, ReadState, WriteCall, WriteOpaque, WriteState,
};
use core::ops::AsyncFnOnce;

impl<S: Send + 'static, Role> ReadState<S, Role> for &AktorTask<S, Role> {
    type Lease = AktorTaskState<S>;
}
impl<S: Send + 'static, Role> WriteState<S, Role> for &AktorTask<S, Role> {
    type Lease = AktorTaskState<S>;
}

impl<'a, S: Send + 'static, I, O: 'static, Role> ReadCall<S, I, O, Role>
    for &'a AktorTask<S, Role>
{
    type Request<Fut> = AktorTaskRequest<'a, S, I, O, Fut, Role>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        _: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        AktorTaskRequest::new(self, operation, input, factory)
    }
}
impl<'a, S: Send + 'static, I, O: 'static, Role> WriteCall<S, I, O, Role>
    for &'a AktorTask<S, Role>
{
    type Request<Fut> = AktorTaskRequest<'a, S, I, O, Fut, Role>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        _: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        AktorTaskRequest::new(self, operation, input, factory)
    }
}

impl<'a, S: Send + 'static, I, Role> ReadOpaque<S, I, Role> for &'a AktorTask<S, Role> {
    type Output<Fut: Future>
        = AktorTaskRequest<'a, S, I, <Fut::Output as CallOutput>::Output, Fut, Role>
    where
        Fut::Output: CallOutput;

    fn dispatch<F, Fut, O>(
        self,
        operation: Operation,
        _: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Output<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        AktorTaskRequest::new(self, operation, input, factory)
    }
}
impl<'a, S: Send + 'static, I, Role> WriteOpaque<S, I, Role> for &'a AktorTask<S, Role> {
    type Output<Fut: Future>
        = AktorTaskRequest<'a, S, I, <Fut::Output as CallOutput>::Output, Fut, Role>
    where
        Fut::Output: CallOutput;

    fn dispatch<F, Fut, O>(
        self,
        operation: Operation,
        _: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Output<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        AktorTaskRequest::new(self, operation, input, factory)
    }
}
