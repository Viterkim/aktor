use super::request::{ReadBody, WriteBody};
use super::*;
use crate::dispatch::{
    CallOutput, OwnedState, Read, ReadCall, ReadOpaque, ReadState, Write, WriteCall, WriteOpaque,
    WriteState,
};
use core::{future::Future, ops::AsyncFnOnce};

impl<S: 'static, Role> ReadState<S, Role> for &Handle<S, Role> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, Role> WriteState<S, Role> for &Handle<S, Role> {
    type Lease = OwnedState<S>;
}
impl<'a, S: 'static, I: 'static, O: 'static, Role> ReadCall<S, I, O, Role> for &'a Handle<S, Role> {
    type Request<Fut> = Request<'a, S, I, O, Role>;

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
        Request::new(self, operation, Box::new(ReadBody(function)), input)
    }
}
impl<'a, S: 'static, I: 'static, O: 'static, Role> WriteCall<S, I, O, Role>
    for &'a Handle<S, Role>
{
    type Request<Fut> = Request<'a, S, I, O, Role>;

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
        Request::new(self, operation, Box::new(WriteBody(function)), input)
    }
}
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> Read<S, I, O, Role>
    for &'a Handle<S, Role>
{
    type Request = Request<'a, S, I, O, Role>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
    {
        Request::new(self, operation, Box::new(ReadBody(function)), input)
    }
}
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> Write<S, I, O, Role>
    for &'a Handle<S, Role>
{
    type Request = Request<'a, S, I, O, Role>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
    {
        Request::new(self, operation, Box::new(WriteBody(function)), input)
    }
}
impl<'a, S: 'static, I: Send + 'static, Role> ReadOpaque<S, I, Role> for &'a Handle<S, Role> {
    type Output<Fut: Future>
        = Request<'a, S, I, <Fut::Output as CallOutput>::Output, Role>
    where
        Fut::Output: CallOutput;

    fn dispatch<F, Fut, O>(
        self,
        operation: Operation,
        function: F,
        input: I,
        _: fn(Self::Lease, I) -> Fut,
    ) -> Self::Output<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        Request::new(self, operation, Box::new(ReadBody(function)), input)
    }
}
impl<'a, S: 'static, I: Send + 'static, Role> WriteOpaque<S, I, Role> for &'a Handle<S, Role> {
    type Output<Fut: Future>
        = Request<'a, S, I, <Fut::Output as CallOutput>::Output, Role>
    where
        Fut::Output: CallOutput;

    fn dispatch<F, Fut, O>(
        self,
        operation: Operation,
        function: F,
        input: I,
        _: fn(Self::Lease, I) -> Fut,
    ) -> Self::Output<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        Request::new(self, operation, Box::new(WriteBody(function)), input)
    }
}
