use super::*;
use crate::{
    dispatch::{OwnedState, Read, ReadCall, ReadState, Write, WriteCall, WriteState},
    listener::Operation,
    message::Request,
};
use core::{future::Future, ops::AsyncFnOnce};

impl<'a, S: 'static, E, C, I: Send + 'static, O: Send + 'static, Role> Read<S, I, O, Role>
    for &'a Aktor<S, E, C, Role>
{
    type Request = Request<'a, S, O>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
    {
        Read::dispatch(&self.handle, operation, function, input)
    }
}
impl<'a, S: 'static, E, C, I: Send + 'static, O: Send + 'static, Role> Write<S, I, O, Role>
    for &'a Aktor<S, E, C, Role>
{
    type Request = Request<'a, S, O>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
    {
        Write::dispatch(&self.handle, operation, function, input)
    }
}

impl<S: 'static, I: Send + 'static, O: Send + 'static, E, C, Role: 'static>
    crate::latest::Session<S, I, O, Role> for &Aktor<S, E, C, Role>
{
    type Sender = crate::message::LatestSender<I>;
    type Results = crate::message::LatestResults<O>;

    fn session<F>(self, operation: Operation, function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> core::ops::AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
    {
        crate::latest::Session::session(&self.handle, operation, function)
    }
}

impl<'a, S: 'static, E, C, I: Send + 'static, O: Send + 'static, Role> ReadCall<S, I, O, Role>
    for &'a Aktor<S, E, C, Role>
{
    type Request<Fut> = Request<'a, S, O>;

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

impl<'a, S: 'static, E, C, I: Send + 'static, O: Send + 'static, Role> WriteCall<S, I, O, Role>
    for &'a Aktor<S, E, C, Role>
{
    type Request<Fut> = Request<'a, S, O>;

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

impl<S: 'static, E, C, Role> ReadState<S, Role> for &Aktor<S, E, C, Role> {
    type Lease = OwnedState<S>;
}
impl<S: 'static, E, C, Role> WriteState<S, Role> for &Aktor<S, E, C, Role> {
    type Lease = OwnedState<S>;
}

impl<S, I, O, Lease, Role, E, C, Wire> crate::latest::TypedSession<S, I, O, Lease, Role, Wire>
    for &Aktor<S, E, C, Role>
where
    Self: crate::latest::Session<S, I, O, Role>,
{
    type Sender<Fut> = <Self as crate::latest::Session<S, I, O, Role>>::Sender;
    type Results = <Self as crate::latest::Session<S, I, O, Role>>::Results;

    fn session<F, Fut>(
        self,
        operation: Operation,
        function: F,
        _: fn(Lease, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: core::future::Future<Output = (Lease, O)>,
    {
        crate::latest::Session::session(self, operation, function)
    }
}
