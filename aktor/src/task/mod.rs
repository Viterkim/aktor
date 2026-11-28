pub mod hooks;

mod dispatch;
mod impls;
mod latest;
mod launch;
mod request;

pub use latest::{AktorTaskLatest, AktorTaskResults};
pub(crate) use launch::ready;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
pub(crate) use launch::start_sync;
#[cfg(feature = "bevy")]
pub(crate) use launch::start_sync_on;
pub use request::{AktorTaskCast, AktorTaskReply, AktorTaskRequest, AktorTaskSend};

use crate::{
    AktorKillSwitch,
    operation::{Operation, hooks::CallHooks},
    queue::HandleInner,
    serve::Clock,
};
use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use pin_project_lite::pin_project;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard, oneshot};

pub struct AktorTask<S, Role = crate::AktorNoRole> {
    inner: Arc<HandleInner<Message<S>>>,
    kill: AktorKillSwitch,
    role: PhantomData<fn() -> Role>,
    clock: Clock,
}

/// The state, leased to one call at a time.
pub struct AktorTaskState<S> {
    guard: OwnedMutexGuard<Option<S>>,
}

/// A task actor's state. A call takes it out as a lease and the next one gets it back.
///
/// The lease only exists because "the future of `async fn(&mut S, I)` is `Send` for every borrow"
/// can't be written down without return type notation. A future that owns its lease is one type
/// the compiler can check. Once return type notation is stable, task calls can run against
/// `&mut S` like thread calls and this goes.
#[doc(hidden)]
pub struct Leased<S> {
    owner: Arc<Mutex<Option<S>>>,
    lease: Option<AktorTaskState<S>>,
}

pub type AktorTaskFuture<'a, O> = Pin<Box<dyn Future<Output = O> + Send + 'a>>;

pub struct Message<S> {
    operation: Operation,
    job: Pin<Box<dyn Job<S> + Send>>,
}

trait Job<S> {
    fn service_id(&self) -> Option<*const ()> {
        None
    }

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        state: Option<AktorTaskState<S>>,
        owner: &Arc<Mutex<Option<S>>>,
        hooks: &mut CallHooks<S>,
        operation: Operation,
    ) -> Poll<Option<AktorTaskState<S>>>;
}

struct Running<'a, S> {
    job: Pin<&'a mut (dyn Job<S> + Send)>,
    state: &'a mut Leased<S>,
    lease: Option<AktorTaskState<S>>,
    waiting: Option<AktorTaskFuture<'static, OwnedMutexGuard<Option<S>>>>,
    hooks: &'a mut CallHooks<S>,
    operation: Operation,
}

pin_project! {
    #[project = CallProj]
    struct Call<S, I, O, Fut> {
        input: Option<I>,
        factory: fn(AktorTaskState<S>, I) -> Fut,
        admission: Arc<crate::queue::Admission>,
        reply: Option<oneshot::Sender<Result<O, crate::CallLoss>>>,
        #[pin]
        future: Option<Fut>,
    }
    impl<S, I, O, Fut> PinnedDrop for Call<S, I, O, Fut> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(reply) = this.reply.take() {
                let _sent = reply.send(Err(crate::CallLoss(if this.input.is_some() {
                    crate::message::AktorDeliveryOutcome::Discarded
                } else {
                    crate::message::AktorDeliveryOutcome::OutcomeUnknown
                }, this.admission.lost.get().cloned())));
            }
        }
    }
}

/// An interval's turn. It gets the lease, and the after hook takes the state back once the
/// interval let go of it.
struct IntervalJob<S> {
    run: Arc<parking_lot::Mutex<crate::AktorClosure<dyn hooks::AktorTaskInterval<S> + Send>>>,
    future: Option<AktorTaskFuture<'static, ()>>,
    relock: Option<AktorTaskFuture<'static, OwnedMutexGuard<Option<S>>>>,
    _due: crate::serve::Due,
}

#[cfg(not(target_family = "wasm"))]
type TaskOwnerFuture = AktorTaskFuture<'static, ()>;
#[cfg(target_family = "wasm")]
type TaskOwnerFuture = crate::message::LocalFuture<'static, ()>;
