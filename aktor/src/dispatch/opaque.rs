use super::*;

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<'a, S: 'static, I: Send + 'static, Role> ReadOpaque<S, I, Role> for &'a Handle<S, Role> {
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
        Queued::read(self, operation, function, input)
    }
}

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<'a, S: 'static, I: Send + 'static, Role> WriteOpaque<S, I, Role> for &'a Handle<S, Role> {
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
        Queued::write(self, operation, function, input)
    }
}

impl<S: 'static, I, Role> ReadOpaque<S, I, Role> for &S {
    type Output<Fut: Future>
        = Direct<Fut>
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
        let _ = (operation, function);
        Direct::new(factory(self, input))
    }
}

impl<S: 'static, I, Role> ReadOpaque<S, I, Role> for &mut S {
    type Output<Fut: Future>
        = Direct<Fut>
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
        let _ = (operation, function);
        Direct::new(factory(self, input))
    }
}

impl<S: 'static, I, Role> WriteOpaque<S, I, Role> for &mut S {
    type Output<Fut: Future>
        = Direct<Fut>
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
        let _ = (operation, function);
        Direct::new(factory(self, input))
    }
}

#[doc(hidden)]
pub trait ReadOpaque<S: 'static, I, Role = ()>: ReadState<S, Role> {
    type Output<Fut: Future>
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
        Fut: Future<Output = (Self::Lease, O)>;
}

#[doc(hidden)]
pub trait WriteOpaque<S: 'static, I, Role = ()>: WriteState<S, Role> {
    type Output<Fut: Future>
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
        Fut: Future<Output = (Self::Lease, O)>;
}
