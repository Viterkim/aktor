use super::*;
use core::{
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

#[doc(hidden)]
pub struct OwnedState<S>(pub S);
impl<S> Deref for OwnedState<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.0
    }
}
impl<S> DerefMut for OwnedState<S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.0
    }
}

#[doc(hidden)]
pub struct Direct<F> {
    future: Pin<Box<F>>,
}
impl<F> Direct<F> {
    pub fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}
impl<F, L, O> Future for Direct<F>
where
    F: Future<Output = (L, O)>,
{
    type Output = O;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        self.future.as_mut().poll(cx).map(|(_, output)| output)
    }
}

#[doc(hidden)]
pub trait ReadState<S: 'static, Role = ()> {
    type Lease: Deref<Target = S>;
}

#[doc(hidden)]
pub trait WriteState<S: 'static, Role = ()> {
    type Lease: DerefMut<Target = S>;
}

#[doc(hidden)]
pub trait ReadCall<S: 'static, I, O: 'static, Role = ()>: ReadState<S, Role> {
    type Request<Fut>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        function: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>;
}

#[doc(hidden)]
pub trait WriteCall<S: 'static, I, O: 'static, Role = ()>: WriteState<S, Role> {
    type Request<Fut>;

    fn dispatch<F, Fut>(
        self,
        operation: Operation,
        function: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Self::Request<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>;
}

impl<'a, S: 'static, Role> ReadState<S, Role> for &'a S {
    type Lease = &'a S;
}
impl<'a, S: 'static, Role> ReadState<S, Role> for &'a mut S {
    type Lease = &'a mut S;
}
impl<'a, S: 'static, Role> WriteState<S, Role> for &'a mut S {
    type Lease = &'a mut S;
}

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<S: 'static, Role> ReadState<S, Role> for &Handle<S, Role> {
    type Lease = OwnedState<S>;
}
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<S: 'static, Role> WriteState<S, Role> for &Handle<S, Role> {
    type Lease = OwnedState<S>;
}

impl<S: 'static, I, O: 'static, Role> ReadCall<S, I, O, Role> for &S {
    type Request<Fut> = Direct<Fut>;

    fn dispatch<F, Fut>(
        self,
        _: Operation,
        _: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Direct<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        Direct::new(factory(self, input))
    }
}
impl<S: 'static, I, O: 'static, Role> ReadCall<S, I, O, Role> for &mut S {
    type Request<Fut> = Direct<Fut>;

    fn dispatch<F, Fut>(
        self,
        _: Operation,
        _: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Direct<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        Direct::new(factory(self, input))
    }
}
impl<S: 'static, I, O: 'static, Role> WriteCall<S, I, O, Role> for &mut S {
    type Request<Fut> = Direct<Fut>;

    fn dispatch<F, Fut>(
        self,
        _: Operation,
        _: F,
        input: I,
        factory: fn(Self::Lease, I) -> Fut,
    ) -> Direct<Fut>
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Send + 'static,
        Fut: Future<Output = (Self::Lease, O)>,
    {
        Direct::new(factory(self, input))
    }
}

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> ReadCall<S, I, O, Role>
    for &'a Handle<S, Role>
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
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl<'a, S: 'static, I: Send + 'static, O: Send + 'static, Role> WriteCall<S, I, O, Role>
    for &'a Handle<S, Role>
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

#[doc(hidden)]
pub trait CallOutput {
    type Output: 'static;
}
impl<L, O: 'static> CallOutput for (L, O) {
    type Output = O;
}
