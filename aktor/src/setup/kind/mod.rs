//! The concrete types behind AktorKind::TokioThread and the other setup choices.
//! Constructors and backend bounds live in impls.rs, AktorExecution lists the report variants.

#[cfg(feature = "local")]
use crate::{AktorSetupError, local::clock::AktorGroupClock, message::LocalFuture};
#[cfg(feature = "local")]
use alloc::rc::Rc;
use core::marker::PhantomData;

mod impls;
pub use crate::AktorExecution;
#[cfg(feature = "local")]
pub use impls::*;

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod tokio {
    #[derive(Clone, Copy, Debug)]
    pub struct TokioThread;

    #[derive(Clone, Copy, Debug)]
    pub struct TokioTask;

    #[cfg(feature = "local")]
    pub struct TokioLocal<'a> {
        pub executor: &'a ::tokio::task::LocalSet,
    }
}
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
pub use tokio::*;

#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
#[derive(Clone, Copy, Debug)]
pub struct BrowserLocal;

#[cfg(feature = "embassy")]
pub struct EmbassyLocal {
    pub spawn: Rc<dyn Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>>,
}

#[cfg(feature = "local")]
pub struct Local<Clock: AktorGroupClock> {
    pub spawn: Rc<dyn Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>>,
    pub clock: PhantomData<fn() -> Clock>,
}

#[cfg(feature = "local")]
pub struct Custom<Clock: AktorGroupClock, Runner> {
    pub spawn: Rc<dyn Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>>,
    pub runner: Runner,
    pub clock: PhantomData<fn() -> Clock>,
}

#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
#[derive(Clone, Copy, Debug)]
pub struct StdThread;

#[cfg(feature = "bevy")]
mod bevy {
    use bevy_tasks::TaskPool;

    pub struct BevyTask<'a> {
        pub executor: &'a TaskPool,
    }

    pub struct BevyLocal<'a> {
        pub executor: &'a TaskPool,
    }
}
#[cfg(feature = "bevy")]
pub use bevy::*;

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub struct BrowserWebWorker<S> {
    pub program: alloc::string::String,
    pub state: PhantomData<fn() -> S>,
}

#[cfg(feature = "embassy_cross_core")]
pub struct EmbassyCrossCore {
    pub spawn: Rc<dyn Fn(LocalFuture<'static, ()>) -> Result<(), AktorSetupError>>,
}

/// Keep typed cleanup data on this execution kind.
pub struct AktorLifecycle<Kind, CleanupData> {
    pub kind: Kind,
    pub data: PhantomData<fn() -> CleanupData>,
}

#[doc(hidden)]
pub trait AktorMode {
    type Each<S: 'static>: ?Sized;
    type End<S: 'static>: ?Sized;
    type Interval<S: 'static>: ?Sized;

    const EXECUTION: AktorExecution;
}
