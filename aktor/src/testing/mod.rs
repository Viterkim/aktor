use crate::{dispatch::Export, message::LocalFuture};
use alloc::{boxed::Box, collections::BTreeMap, string::String};
use core::{any::Any, marker::PhantomData, ops::AsyncFnMut};

mod impls;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
mod native;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
mod request;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub use native::Mock;
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub use request::{LatestResults, Reply, Request};

/// Give functions test responses. Each actor gets its own handlers.
pub struct Preset<S, Role = ()> {
    handlers: Handlers,
    state: PhantomData<fn() -> (S, Role)>,
}

pub struct Handler<E, F, C = crate::dispatch::DataCodec> {
    function: F,
    export: PhantomData<fn() -> (E, C)>,
}

#[derive(Debug)]
pub struct MissingHandler {
    pub operation: String,
}

type Value = Box<dyn Any>;

#[doc(hidden)]
#[derive(Default)]
pub struct Handlers {
    entries: BTreeMap<&'static str, Box<dyn Run + Send>>,
}

trait Run {
    fn run(&mut self, input: Value) -> LocalFuture<'_, Result<Value, MissingHandler>>;
}

pub fn handler<E, F, C>(_: E, function: F) -> Handler<E, F, C>
where
    E: Export<C>,
    F: AsyncFnMut(E::Input) -> E::Output + Send + 'static,
{
    Handler {
        function,
        export: PhantomData,
    }
}
