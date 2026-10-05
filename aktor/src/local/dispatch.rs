use super::*;
use crate::dispatch::{OwnedState, Read, ReadCall, ReadState, Write, WriteCall, WriteState};
use core::{future::Future, ops::AsyncFnOnce};

impl<'a, S: 'static, const N: usize, E, I: 'static, O: 'static, Role, Clock> Read<S, I, O, Role>
    for &'a Handle<S, N, E, Role, Clock>
{
    type Request = Request<'a, S, N, E, O, Role, Clock>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
    {
        Request::new(
            self,
            operation,
            async move |state: &mut S, input| function(state, input).await,
            input,
        )
    }
}
impl<'a, S: 'static, const N: usize, E, I: 'static, O: 'static, Role, Clock> Write<S, I, O, Role>
    for &'a Handle<S, N, E, Role, Clock>
{
    type Request = Request<'a, S, N, E, O, Role, Clock>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
    {
        Request::new(self, operation, function, input)
    }
}

impl<'a, S: 'static, const N: usize, E, I: 'static, O: 'static, Role, Clock> ReadCall<S, I, O, Role>
    for &'a Handle<S, N, E, Role, Clock>
{
    type Request<Fut> = Request<'a, S, N, E, O, Role, Clock>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        function: F,
        input: I,
        _: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        Read::dispatch(self, operation, function, input)
    }
}

impl<'a, S: 'static, const N: usize, E, I: 'static, O: 'static, Role, Clock>
    WriteCall<S, I, O, Role> for &'a Handle<S, N, E, Role, Clock>
{
    type Request<Fut> = Request<'a, S, N, E, O, Role, Clock>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        function: F,
        input: I,
        _: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        Write::dispatch(self, operation, function, input)
    }
}

impl<S: 'static, const N: usize, E, Role, Clock> ReadState<S, Role>
    for &Handle<S, N, E, Role, Clock>
{
    type Lease = OwnedState<S>;
}
impl<S: 'static, const N: usize, E, Role, Clock> WriteState<S, Role>
    for &Handle<S, N, E, Role, Clock>
{
    type Lease = OwnedState<S>;
}
