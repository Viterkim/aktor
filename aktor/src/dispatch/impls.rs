use super::*;

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> Read<S, I, O, Role>
    for &'a Handle<S, Role>
{
    type Request = Request<'a, S, O>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
    {
        call_async(
            self,
            async move |state: &mut S, input| function(state, input).await,
            input,
        )
        .operation(operation)
    }
}
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> Write<S, I, O, Role>
    for &'a Handle<S, Role>
{
    type Request = Request<'a, S, O>;

    fn dispatch<F>(self, operation: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
    {
        call_async(self, function, input).operation(operation)
    }
}

impl<'a, S: 'static, I: 'a, O: 'static, Role> Read<S, I, O, Role> for &'a S {
    type Request = LocalFuture<'a, O>;

    fn dispatch<F>(self, _: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
    {
        Box::pin(async move { function(self, input).await })
    }
}

impl<'a, S: 'static, I: 'a, O: 'static, Role> Read<S, I, O, Role> for &'a mut S {
    type Request = LocalFuture<'a, O>;

    fn dispatch<F>(self, _: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
    {
        Box::pin(async move { function(self, input).await })
    }
}
impl<'a, S: 'static, I: 'a, O: 'static, Role> Write<S, I, O, Role> for &'a mut S {
    type Request = LocalFuture<'a, O>;

    fn dispatch<F>(self, _: Operation, function: F, input: I) -> Self::Request
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
    {
        Box::pin(async move { function(self, input).await })
    }
}
