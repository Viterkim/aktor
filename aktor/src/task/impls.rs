use super::*;
use crate::{message::consumed, operation::hooks::AktorHooks};
use core::ops::{Deref, DerefMut};

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

impl<S> Deref for AktorTaskState<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.guard
    }
}
impl<S> DerefMut for AktorTaskState<S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.guard
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
        hooks: &mut AktorHooks<S>,
        operation: Operation,
    ) -> Poll<()> {
        let mut this = self.project();

        if let Some(mut state) = state {
            hooks.before(&mut state, operation);

            let Some(input) = this.input.take() else {
                consumed();
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
