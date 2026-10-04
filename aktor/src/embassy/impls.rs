use super::*;
use core::future::{IntoFuture, poll_fn};

/// Capacity bounds queued work. One operation can also be running.
pub fn channel<S, const N: usize, E>() -> Result<Channel<S, N, E>, ActorError> {
    if N == 0 {
        return Err(ActorError::InvalidCapacity);
    }

    let inner = Rc::new(Inner {
        queue: RefCell::new(VecDeque::new()),
        services: RefCell::new(VecDeque::new()),
        prefer_service: Cell::new(false),
        group: RefCell::new(None),
        sessions: RefCell::new(alloc::vec::Vec::new()),
        open: Cell::new(true),
        handles: Cell::new(1),
        closed: Event::default(),
        completion: Rc::new(Completed {
            ready: RefCell::new(None),
            result: RefCell::new(None),
            changed: Event::default(),
        }),
    });

    Ok((
        Handle {
            inner: inner.clone(),
            role: PhantomData,
        },
        Owner { inner },
    ))
}

impl<S, const N: usize, E> Inner<S, N, E> {
    fn close(&self) {
        if self.open.replace(false) {
            self.closed.notify();
            self.wake_sessions();
        }
    }

    fn wake_sessions(&self) {
        let callbacks = self.sessions.borrow().clone();
        let expired: alloc::vec::Vec<_> = callbacks.into_iter().filter(|wake| !wake()).collect();
        self.sessions
            .borrow_mut()
            .retain(|wake| !expired.iter().any(|dead| Rc::ptr_eq(wake, dead)));
    }

    pub fn lost(&self) -> bool {
        if let Some((name, group)) = &*self.group.borrow() {
            group.fail(crate::ActorFailure {
                actor: name.clone(),
                phase: "call".into(),
                message: "actor stopped without an output".into(),
            });
            true
        } else {
            false
        }
    }

    pub fn service(&self, message: Message<S>) {
        self.services.borrow_mut().push_back(message);
        self.closed.notify();
    }

    pub fn enqueue(&self, message: Message<S>) -> Result<(), Message<S>> {
        let mut queue = self.queue.borrow_mut();
        if queue.len() == N {
            return Err(message);
        }
        queue.push_back(message);
        drop(queue);
        self.closed.notify();
        Ok(())
    }

    fn finish(&self, result: Result<(), Rc<OwnerError<E>>>) {
        if let Err(error) = &result
            && let Some((name, group)) = &*self.group.borrow()
        {
            group.fail(crate::ActorFailure {
                actor: name.clone(),
                phase: match &**error {
                    OwnerError::Setup(_) => "setup",
                    OwnerError::Cleanup(_) => "cleanup",
                    OwnerError::Cancelled => "owner",
                }
                .into(),
                message: alloc::format!("{error}"),
            });
        }
        self.close();

        if self.completion.result.borrow().is_some() {
            return;
        }

        let queued = core::mem::take(&mut *self.queue.borrow_mut());
        let services = core::mem::take(&mut *self.services.borrow_mut());
        drop(queued);
        drop(services);
        if self.completion.ready.borrow().is_none() {
            *self.completion.ready.borrow_mut() = Some(result.clone());
        }
        *self.completion.result.borrow_mut() = Some(result);
        self.completion.changed.notify();
        self.wake_sessions();
    }
}

impl<S, const N: usize, E, Role> Handle<S, N, E, Role> {
    pub fn downgrade(&self) -> WeakHandle<S, N, E, Role> {
        WeakHandle {
            inner: Rc::downgrade(&self.inner),
            role: PhantomData,
        }
    }

    /// Wait for setup, or get its error.
    pub async fn ready(&self) -> Result<(), Rc<OwnerError<E>>> {
        let changed = self.inner.completion.changed.listen();

        poll_fn(|context| {
            changed.register(context);
            match self.inner.completion.ready.borrow().clone() {
                Some(result) => Poll::Ready(result),
                None => Poll::Pending,
            }
        })
        .await
    }

    pub fn new_handle(&self) -> Self {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Self {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }

    pub fn with_role<NewRole>(self) -> Handle<S, N, E, NewRole> {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Handle {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }

    /// Start closing now, queued calls finish before cleanup.
    pub fn shutdown(&self) -> Completion<E> {
        self.inner.close();
        self.completion()
    }

    pub fn completion(&self) -> Completion<E> {
        Completion {
            inner: self.inner.completion.clone(),
        }
    }
}
impl<S, const N: usize, E, Role> Drop for Handle<S, N, E, Role> {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;
        self.inner.handles.set(handles);

        if handles == 0 {
            self.inner.close();
        }
    }
}

impl<S, const N: usize, E, Role> WeakHandle<S, N, E, Role> {
    pub fn upgrade(&self) -> Option<Handle<S, N, E, Role>> {
        let inner = self.inner.upgrade()?;

        if !inner.open.get() {
            return None;
        }

        inner.handles.set(inner.handles.get() + 1);
        Some(Handle {
            inner,
            role: PhantomData,
        })
    }
}
impl<S, const N: usize, E, Role> Clone for WeakHandle<S, N, E, Role> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }
}

impl<S, const N: usize, E> Owner<S, N, E> {
    pub fn completion(&self) -> Completion<E> {
        Completion {
            inner: self.inner.completion.clone(),
        }
    }

    /// Run setup here on the owner's task.
    pub async fn run_with<Setup, SetupFuture, Cleanup, CleanupFuture>(
        self,
        setup: Setup,
        cleanup: Cleanup,
    ) -> Result<(), Rc<OwnerError<E>>>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, crate::AktorSetupError<E>>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<E>>>,
    {
        match setup().await {
            Ok(state) => self.run(state, cleanup).await,
            Err(error) => {
                let result = Err(Rc::new(OwnerError::Setup(error)));
                self.inner.finish(result.clone());
                result
            }
        }
    }

    /// Each operation completes before the next starts. Shutdown also awaits cleanup.
    pub async fn run<Cleanup, CleanupFuture>(
        self,
        mut state: S,
        cleanup: Cleanup,
    ) -> Result<(), Rc<OwnerError<E>>>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<E>>>,
    {
        *self.inner.completion.ready.borrow_mut() = Some(Ok(()));
        self.inner.completion.changed.notify();

        let closing = self.inner.closed.listen();
        loop {
            let message = poll_fn(|context| {
                if !self.inner.open.get()
                    && self.inner.queue.borrow().is_empty()
                    && self.inner.services.borrow().is_empty()
                {
                    return Poll::Ready(None);
                }

                closing.register(context);
                if (self.inner.prefer_service.get() || self.inner.queue.borrow().is_empty())
                    && let Some(message) = self.inner.services.borrow_mut().pop_front()
                {
                    self.inner.prefer_service.set(false);
                    return Poll::Ready(Some(message));
                }
                let message = self.inner.queue.borrow_mut().pop_front();
                match message {
                    Some(message) => {
                        self.inner.prefer_service.set(true);
                        self.inner.closed.notify();
                        Poll::Ready(Some(message))
                    }
                    None => Poll::Pending,
                }
            })
            .await;

            let Some(mut message) = message else { break };
            message.job.run(&mut state).await;
            let mut yielded = false;
            poll_fn(|cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }

        let result = cleanup(state)
            .await
            .map_err(|error| Rc::new(OwnerError::Cleanup(error)));
        self.inner.finish(result.clone());
        result
    }
}
impl<S, const N: usize, E> Drop for Owner<S, N, E> {
    fn drop(&mut self) {
        if self.inner.completion.result.borrow().is_none() {
            self.inner.finish(Err(Rc::new(OwnerError::Cancelled)));
        }
    }
}

impl<E> Completion<E> {
    pub fn new_observer(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    pub async fn wait(&self) -> Result<(), Rc<OwnerError<E>>> {
        let changed = self.inner.changed.listen();

        poll_fn(|context| {
            changed.register(context);
            match self.inner.result.borrow().clone() {
                Some(result) => Poll::Ready(result),
                None => Poll::Pending,
            }
        })
        .await
    }
}
impl<E: 'static> IntoFuture for Completion<E> {
    type Output = Result<(), Rc<OwnerError<E>>>;
    type IntoFuture = LocalFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}
impl<'a, E: 'a> IntoFuture for &'a Completion<E> {
    type Output = Result<(), Rc<OwnerError<E>>>;
    type IntoFuture = LocalFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.wait())
    }
}

impl<E> core::fmt::Display for OwnerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Setup(error) => write!(f, "actor setup failed: {error}"),
            Self::Cleanup(error) => write!(f, "actor cleanup failed: {error}"),
            Self::Cancelled => f.write_str("actor owner stopped before cleanup completed"),
        }
    }
}
impl<E> core::fmt::Debug for OwnerError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(self, f)
    }
}
impl<E: 'static> core::error::Error for OwnerError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Setup(error) | Self::Cleanup(error) => Some(error),
            Self::Cancelled => None,
        }
    }
}
