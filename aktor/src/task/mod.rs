use crate::{
    KillSwitch,
    operation::{Operation, hooks::AktorHooks},
};
use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use pin_project_lite::pin_project;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMappedMutexGuard, mpsc, oneshot, watch};

mod clock;
mod dispatch;
mod driver;
pub mod hooks;
mod impls;
mod join;
mod latest;
mod request;
mod service;

#[doc(hidden)]
pub use clock::TaskClock;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
pub use driver::start;
#[doc(hidden)]
pub use driver::start_on;
#[doc(hidden)]
pub use join::TaskJoin;
pub use latest::{AktorTaskLatest, AktorTaskResults};
pub use request::{AktorTaskReply, AktorTaskRequest};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorTaskStatus {
    Running,
    Finished,
    Failed,
}

pub struct AktorTask<S, Role = crate::AktorNoRole> {
    pub name: Arc<String>,
    sender: mpsc::Sender<Message<S>>,
    services: Arc<service::Services<S>>,
    kill: KillSwitch,
    finished: watch::Receiver<AktorTaskStatus>,
    role: PhantomData<fn() -> Role>,
    clock: TaskClock,
}

pub struct AktorTaskState<S> {
    guard: OwnedMappedMutexGuard<Option<S>, S>,
}

pub type AktorTaskFuture<'a, O> = Pin<Box<dyn Future<Output = O> + Send + 'a>>;

pub struct Message<S> {
    operation: Operation,
    job: Box<dyn Job<S> + Send>,
}

trait Job<S> {
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        state: Option<AktorTaskState<S>>,
        hooks: &mut AktorHooks<S>,
        operation: Operation,
    ) -> Poll<()>;
}

pin_project! {
    struct Call<S, I, O, Fut> {
        input: Option<I>,
        factory: fn(AktorTaskState<S>, I) -> Fut,
        reply: Option<oneshot::Sender<O>>,
        #[pin]
        future: Option<Fut>,
    }
}

#[cfg(not(target_family = "wasm"))]
type TaskOwnerFuture = AktorTaskFuture<'static, ()>;
#[cfg(target_family = "wasm")]
type TaskOwnerFuture = crate::message::LocalFuture<'static, ()>;
