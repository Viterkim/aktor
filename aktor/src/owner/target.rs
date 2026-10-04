use super::*;
use crate::{
    listener::Operation,
    message::Request,
    target::{Read, Write},
};
use core::ops::AsyncFnOnce;

impl<'a, S: 'static, E, C, I: Send + 'static, Role> Read<S, I, Role> for &'a Aktor<S, E, C, Role> {
    type Output<O: Send + 'static> = Request<'a, S, O>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        Read::dispatch(&self.handle, operation, function, input)
    }
}
impl<'a, S: 'static, E, C, I: Send + 'static, Role> Write<S, I, Role> for &'a Aktor<S, E, C, Role> {
    type Output<O: Send + 'static> = Request<'a, S, O>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: Send + 'static,
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
