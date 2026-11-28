mod impls;
#[cfg(test)]
mod tests;

pub use crate::operation::Operation;
pub use crate::operation::hooks;
pub use crate::owner_tasks::{
    AktorOwnerTask, AktorStopping, aktor_spawn, aktor_spawn_local, aktor_stopping,
};
pub use crate::panic::Failure;

use crate::message::Message;
use crate::queue::HandleInner;
use core::marker::PhantomData;
use er::*;
use std::{
    sync::{Arc, Weak},
    thread,
};
use tokio::sync::watch;

pub struct Handle<S, Role = ()> {
    pub(crate) inner: Arc<HandleInner<Message<S>>>,
    role: PhantomData<fn() -> Role>,
}

pub struct WeakHandle<S, Role = ()> {
    inner: Weak<HandleInner<Message<S>>>,
    role: PhantomData<fn() -> Role>,
}

#[doc(hidden)]
pub struct Dedicated<S> {
    thread: OwnerThread<Option<S>>,
    finished: watch::Receiver<bool>,
}

#[cfg(not(target_family = "wasm"))]
type ThreadLife = crate::group::ThreadLife;
/// The browser group doesn't count threads, there's nothing to hold.
#[cfg(target_family = "wasm")]
struct ThreadLife;

/// An owner thread with its entry in the group's list of running threads. The entry stays
/// until the OS join returns, and that join waits for thread local destructors too.
struct OwnerThread<T> {
    thread: Option<thread::JoinHandle<T>>,
    life: Option<ThreadLife>,
    reap: fn(thread::JoinHandle<T>, ThreadLife),
}

#[derive(Clone)]
pub struct CompletionObserver {
    finished: watch::Receiver<bool>,
}

#[derive(Er)]
#[er(display = Self::fmt_display)]
pub struct DedicatedJoinError {
    /// The OS join completed, including a thread that panicked.
    pub physically_joined: bool,
    pub payload: parking_lot::Mutex<Box<dyn std::any::Any + Send>>,
}
