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
pub use native::{Mock, call};

use crate::{dispatch::Export, message::LocalFuture};
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{
    any::{Any, TypeId},
    marker::PhantomData,
    ops::AsyncFnMut,
};

/// Give functions test responses. Each actor gets its own handlers.
pub struct Preset<S, Role = ()> {
    handlers: Handlers,
    state: PhantomData<fn() -> (S, Role)>,
}

pub struct Handler<S, Role> {
    entry: Entry,
    state: PhantomData<fn() -> (S, Role)>,
}

struct Callback<E, F, C> {
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
    entries: Vec<Entry>,
}

struct Entry {
    name: &'static str,
    bodies: [TypeId; 2],
    handler: Box<dyn Run + Send>,
}

trait Run {
    fn run(&mut self, input: Value) -> LocalFuture<'_, Result<Value, MissingHandler>>;
}

#[doc(hidden)]
pub fn handler<E, F, C>(_: E, bodies: [TypeId; 2], function: F) -> Handler<E::State, E::Role>
where
    E: Export<C> + 'static,
    E::Input: 'static,
    E::Output: 'static,
    C: 'static,
    F: AsyncFnMut(E::Input) -> E::Output + Send + 'static,
{
    Handler {
        entry: Entry {
            bodies,
            name: E::NAME,
            handler: Box::new(Callback::<E, F, C> {
                function,
                export: PhantomData,
            }),
        },
        state: PhantomData,
    }
}
