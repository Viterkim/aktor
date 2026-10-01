use super::*;

#[cfg(feature = "tokio")]
impl<'a, S: 'static, I: Send + 'static, Role> Read<S, I, Role> for &'a Handle<S, Role> {
    type Output<O: Send + 'static> = Request<'a, S, O>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        call_async(
            self,
            async move |state: &mut S, input| function(state, input).await,
            input,
        )
        .operation(operation)
    }
}
#[cfg(feature = "tokio")]
impl<'a, S: 'static, I: Send + 'static, Role> Write<S, I, Role> for &'a Handle<S, Role> {
    type Output<O: Send + 'static> = Request<'a, S, O>;

    fn dispatch<F, O>(self, operation: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        call_async(self, function, input).operation(operation)
    }
}

impl<'a, S: 'static, I: 'a, Role> Read<S, I, Role> for &'a S {
    type Output<O: Send + 'static> = LocalFuture<'a, O>;

    fn dispatch<F, O>(self, _: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        Box::pin(async move { function(self, input).await })
    }
}

impl<'a, S: 'static, I: 'a, Role> Read<S, I, Role> for &'a mut S {
    type Output<O: Send + 'static> = LocalFuture<'a, O>;

    fn dispatch<F, O>(self, _: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        Box::pin(async move { function(self, input).await })
    }
}
impl<'a, S: 'static, I: 'a, Role> Write<S, I, Role> for &'a mut S {
    type Output<O: Send + 'static> = LocalFuture<'a, O>;

    fn dispatch<F, O>(self, _: Operation, function: F, input: I) -> Self::Output<O>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        O: Send + 'static,
    {
        Box::pin(async move { function(self, input).await })
    }
}
