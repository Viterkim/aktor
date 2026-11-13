use super::*;
use crate::{message::consumed, operation::hooks::CallHooks};
use core::ops::{Deref, DerefMut};

impl<S, Role> AktorTask<S, Role> {
    fn new_handle(&self) -> Self {
        Self {
            cleanup: self.cleanup.clone(),
            name: self.name.clone(),
            sender: self.sender.clone(),
            slots: self.slots.clone(),
            services: self.services.clone(),
            kill: self.kill.clone(),
            finished: self.finished.clone(),
            role: PhantomData,
            clock: self.clock,
        }
    }

    pub fn with_role<NewRole>(self) -> AktorTask<S, NewRole> {
        AktorTask {
            cleanup: self.cleanup,
            name: self.name,
            sender: self.sender,
            slots: self.slots,
            services: self.services,
            kill: self.kill,
            finished: self.finished,
            role: PhantomData,
            clock: self.clock,
        }
    }

    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        self.kill.run(work).await
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped. Returns `None` if the actor has no group.
    pub fn group_completion(&self) -> Option<crate::AktorGroupCompletion> {
        Some(self.kill.completion())
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

impl<S> Deref for AktorTaskState<S> {
    type Target = S;

    fn deref(&self) -> &S {
        self.guard.as_ref().unwrap_or_else(|| consumed())
    }
}
impl<S> DerefMut for AktorTaskState<S> {
    fn deref_mut(&mut self) -> &mut S {
        self.guard.as_mut().unwrap_or_else(|| consumed())
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
        owner: &Arc<Mutex<Option<S>>>,
        hooks: &mut CallHooks<S>,
        operation: Operation,
    ) -> Poll<Option<AktorTaskState<S>>> {
        let mut this = self.project();

        if let Some(mut state) = state {
            hooks.before(&mut state, operation);

            let Some(input) = this.input.take() else {
                consumed();
            };

            this.future.set(Some((this.factory)(state, input)));
        }

        let Some(future) = this.future.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        let (mut state, output) = core::task::ready!(future.poll(cx));

        this.future.set(None);
        if Arc::ptr_eq(OwnedMutexGuard::mutex(&state.guard), owner) {
            hooks.after(&mut state, operation);
        }

        if let Some(reply) = this.reply.take() {
            let _sent = reply.send(output);
        }

        Poll::Ready(Some(state))
    }
}
