use super::super::*;
use crate::message::AktorStateError;

impl<S, Role> Handle<S, Role> {
    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        match self.inner.admission.group() {
            Some(kill) => kill.run(work).await,
            None => Some(work.await),
        }
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped. Returns `None` if the actor has no group.
    pub fn group_completion(&self) -> Option<crate::AktorGroupCompletion> {
        self.inner.admission.group().map(|kill| kill.completion())
    }

    pub fn is_closed(&self) -> bool {
        self.inner.sender.is_closed()
    }

    pub fn capacity(&self) -> usize {
        self.inner.sender.capacity()
    }

    /// Another handle to the same actor.
    fn new_handle(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }

    /// A reference that does not keep the actor alive.
    pub fn downgrade(&self) -> WeakHandle<S, Role> {
        WeakHandle {
            inner: Arc::downgrade(&self.inner),
            role: PhantomData,
        }
    }

    /// Change the type role, keeping the same queue.
    pub fn with_role<R>(self) -> Handle<S, R> {
        Handle {
            inner: self.inner,
            role: PhantomData,
        }
    }

    /// Wait until the listener and its cleanup have finished.
    pub async fn closed(&self) {
        self.completion().wait().await;
    }

    #[doc(hidden)]
    pub async fn wait_closing(&self) {
        let mut changed = self.inner.admission.watch();
        let mut finished = self.inner.finished.clone();

        loop {
            if self.inner.admission.phase().is_closing() || self.inner.sender.is_closed() {
                return;
            }

            tokio::select! {
                _ = changed.changed() => {},
                _ = finished.changed() => return,
            }
        }
    }

    pub fn completion(&self) -> CompletionObserver {
        CompletionObserver {
            finished: self.inner.finished.clone(),
        }
    }
}

impl<S, Role> WeakHandle<S, Role> {
    pub fn upgrade(&self) -> Result<Handle<S, Role>, AktorStateError> {
        let inner = self.inner.upgrade().ok_or(AktorStateError::Closed)?;

        if inner.sender.is_closed() {
            return Err(AktorStateError::Closed);
        }

        Ok(Handle {
            inner,
            role: PhantomData,
        })
    }
}
impl<S, Role> Clone for Handle<S, Role> {
    fn clone(&self) -> Self {
        self.new_handle()
    }
}
impl<S, Role> Clone for WeakHandle<S, Role> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }
}
impl<S, Role> core::fmt::Debug for WeakHandle<S, Role> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("WeakHandle")
            .field("state", &core::any::type_name::<S>())
            .field("strong_handles", &self.inner.strong_count())
            .finish()
    }
}

impl<S, Role> core::fmt::Debug for Handle<S, Role> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Handle")
            .field("state", &core::any::type_name::<S>())
            .field("closed", &self.is_closed())
            .finish()
    }
}
