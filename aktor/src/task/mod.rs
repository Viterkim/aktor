use crate::{KillSwitch, operation::Operation};
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
mod driver;
pub mod hooks;
mod join;
mod latest;
mod request;
mod service;
pub use latest::{AktorTaskLatest, AktorTaskResults};
mod dispatch;

#[doc(hidden)]
pub use clock::TaskClock;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
pub use driver::start;
#[doc(hidden)]
pub use driver::start_on;
#[doc(hidden)]
pub use join::TaskJoin;
pub use request::{AktorTaskReply, AktorTaskRequest};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AktorTaskStatus {
    Running,
    Finished,
    Failed,
}

pub struct AktorTask<S, Role = crate::AktorNoRole> {
    name: Arc<String>,
    sender: mpsc::Sender<Message<S>>,
    services: Arc<service::Services<S>>,
    kill: KillSwitch,
    finished: watch::Receiver<AktorTaskStatus>,
    role: PhantomData<fn() -> Role>,
    clock: TaskClock,
}
impl<S, Role> AktorTask<S, Role> {
    pub fn new_handle(&self) -> Self {
        Self {
            name: self.name.clone(),
            sender: self.sender.clone(),
            services: self.services.clone(),
            kill: self.kill.clone(),
            finished: self.finished.clone(),
            role: PhantomData,
            clock: self.clock,
        }
    }

    pub fn with_role<NewRole>(self) -> AktorTask<S, NewRole> {
        AktorTask {
            name: self.name,
            sender: self.sender,
            services: self.services,
            kill: self.kill,
            finished: self.finished,
            role: PhantomData,
            clock: self.clock,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub async fn closed(&self) {
        self.sender.closed().await;
    }

    pub async fn finished(&self) {
        let mut finished = self.finished.clone();

        while *finished.borrow_and_update() == AktorTaskStatus::Running {
            if finished.changed().await.is_err() {
                return;
            }
        }
    }
}
impl<S, Role> Clone for AktorTask<S, Role> {
    fn clone(&self) -> Self {
        self.new_handle()
    }
}

pub struct AktorTaskState<S> {
    guard: OwnedMappedMutexGuard<Option<S>, S>,
}
impl<S> core::ops::Deref for AktorTaskState<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.guard
    }
}
impl<S> core::ops::DerefMut for AktorTaskState<S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.guard
    }
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
        hooks: &mut crate::operation::hooks::AktorHooks<S>,
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
impl<S, I, O, Fut> Job<S> for Call<S, I, O, Fut>
where
    S: Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    Fut: Future<Output = (AktorTaskState<S>, O)> + Send + 'static,
{
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        state: Option<AktorTaskState<S>>,
        hooks: &mut crate::operation::hooks::AktorHooks<S>,
        operation: Operation,
    ) -> Poll<()> {
        let mut this = self.project();

        if let Some(mut state) = state {
            hooks.before(&mut state, operation);

            let Some(input) = this.input.take() else {
                crate::message::consumed();
            };

            this.future.set(Some((this.factory)(state, input)));
        }

        let Some(future) = this.future.as_mut().as_pin_mut() else {
            return Poll::Ready(());
        };
        let (mut state, output) = core::task::ready!(future.poll(cx));

        this.future.set(None);
        hooks.after(&mut state, operation);

        if let Some(reply) = this.reply.take() {
            let _sent = reply.send(output);
        }

        Poll::Ready(())
    }
}

#[cfg(not(target_family = "wasm"))]
type TaskOwnerFuture = AktorTaskFuture<'static, ()>;
#[cfg(target_family = "wasm")]
type TaskOwnerFuture = crate::message::LocalFuture<'static, ()>;
