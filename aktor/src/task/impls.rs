use super::*;
use crate::{
    message::consumed,
    queue::{self, Service},
    serve::Mail,
};
use core::ops::{Deref, DerefMut};

impl<S, Role> AktorTask<S, Role> {
    pub(crate) fn closed_handle(kill: AktorKillSwitch, clock: Clock) -> Self {
        let (inner, mailbox) = queue::closed_channel();

        inner.admission.shutdown();
        drop(mailbox);

        Self {
            inner,
            kill,
            role: PhantomData,
            clock,
        }
    }

    fn new_handle(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            kill: self.kill.clone(),
            role: PhantomData,
            clock: self.clock,
        }
    }

    pub fn with_role<NewRole>(self) -> AktorTask<S, NewRole> {
        AktorTask {
            inner: self.inner,
            kill: self.kill,
            role: PhantomData,
            clock: self.clock,
        }
    }

    #[doc(hidden)]
    pub fn killswitch(&self) -> AktorKillSwitch {
        self.kill.clone()
    }

    /// Run a job beside this actor's calls. Its capacity bounds the number of unfinished jobs.
    #[track_caller]
    pub fn spawn<O, E>(
        &self,
        work: impl Future<Output = O> + Send + 'static,
        policy: crate::AktorJob,
    ) -> Result<crate::AktorOwnerTask<O::Value>, (crate::AktorSpawnError, String)>
    where
        O: crate::AktorJobOutput<E> + 'static,
        O::Value: Send + 'static,
    {
        self.inner.spawn(work, policy)
    }

    /// Use this just before returning a final reply when the state can no longer serve calls.
    /// Starts group shutdown. This actor discards queued calls and runs `end` after the current call.
    pub fn retire(&self) {
        self.kill.retire_self();
        self.inner.retire();
    }

    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub fn run<F: IntoFuture>(&self, work: F) -> crate::AktorRun<F::IntoFuture> {
        let work = work.into_future();

        self.kill.run(work)
    }

    /// The registered launch attempt this handle belongs to.
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        self.kill.registered_id()
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped.
    pub fn group_completion(&self) -> crate::AktorGroupCompletion {
        self.kill.completion()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.sender.is_closed()
    }

    /// Waits until the actor stops taking calls.
    pub async fn closed(&self) {
        let mut changed = self.inner.admission.watch();
        let mut finished = self.inner.finished.clone();

        while !self.inner.admission.phase().is_closing() && !self.inner.sender.is_closed() {
            tokio::select! {
                _ = changed.changed() => {},
                _ = finished.changed() => return,
            }
        }
    }

    /// Waits until the actor is gone, `end` included.
    pub async fn finished(&self) {
        let _gone = self.inner.finished.clone().changed().await;
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

impl<S> Leased<S> {
    /// The state back for `end`, once whatever holds the lease let go of it.
    pub async fn into_inner(self) -> Option<S> {
        match self.lease {
            Some(mut lease) => lease.guard.take(),
            None => self.owner.lock().await.take(),
        }
    }
}

impl<S> Service for Message<S> {
    fn service_id(&self) -> Option<*const ()> {
        self.job.service_id()
    }
}

impl<S: Send + 'static> Mail<S> for Message<S> {
    type State = Leased<S>;

    fn operation(&self) -> Operation {
        self.operation
    }

    fn run<'a>(
        &'a mut self,
        state: &'a mut Leased<S>,
        hooks: &'a mut CallHooks<S>,
    ) -> impl Future<Output = ()> + 'a {
        let (lease, waiting) = match state.lease.take() {
            Some(lease) => (Some(lease), None),
            None => match state.owner.clone().try_lock_owned() {
                Ok(guard) => (Some(AktorTaskState { guard }), None),
                Err(_) => (
                    None,
                    Some(Box::pin(state.owner.clone().lock_owned()) as AktorTaskFuture<'static, _>),
                ),
            },
        };

        Running {
            job: self.job.as_mut(),
            state,
            lease,
            waiting,
            hooks,
            operation: self.operation,
        }
    }
}

impl<S: Send + 'static> Future for Running<'_, S> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        if this.lease.is_some() {
            #[cfg(feature = "tokio")]
            core::task::ready!(core::pin::pin!(tokio::task::coop::consume_budget()).poll(cx));
        } else if let Some(waiting) = &mut this.waiting {
            let guard = core::task::ready!(waiting.as_mut().poll(cx));

            this.waiting = None;
            this.lease = Some(AktorTaskState { guard });
        }

        let lease = core::task::ready!(this.job.as_mut().poll(
            cx,
            this.lease.take(),
            &this.state.owner,
            this.hooks,
            this.operation
        ));

        // A manually built request can return another actor's state.
        this.state.lease = lease
            .filter(|lease| Arc::ptr_eq(OwnedMutexGuard::mutex(&lease.guard), &this.state.owner));

        Poll::Ready(())
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
            let _sent = reply.send(Ok(output));
        }

        Poll::Ready(Some(state))
    }
}

impl<S: Send + 'static> Job<S> for IntervalJob<S> {
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        state: Option<AktorTaskState<S>>,
        owner: &Arc<Mutex<Option<S>>>,
        hooks: &mut CallHooks<S>,
        operation: Operation,
    ) -> Poll<Option<AktorTaskState<S>>> {
        let this = self.get_mut();

        if let Some(mut state) = state {
            hooks.before(&mut state, operation);
            this.future = Some(this.run.lock().0.run(state));
        }

        if let Some(future) = &mut this.future {
            core::task::ready!(future.as_mut().poll(cx));

            this.future = None;
            this.relock = Some(Box::pin(owner.clone().lock_owned()));
        }

        let Some(relock) = &mut this.relock else {
            return Poll::Ready(None);
        };
        let mut state = AktorTaskState {
            guard: core::task::ready!(relock.as_mut().poll(cx)),
        };

        this.relock = None;

        if state.guard.is_some() {
            hooks.after(&mut state, operation);
        }

        Poll::Ready(Some(state))
    }
}
