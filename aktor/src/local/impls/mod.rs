use super::*;
use core::future::{IntoFuture, poll_fn};

pub mod runner;

/// Capacity bounds queued work. One operation can also be running.
pub fn channel<S, const N: usize, E>() -> Result<Channel<S, N, E>, ActorError> {
    channel_with_capacity(N)
}

pub fn channel_with_capacity<S, const N: usize, E>(
    capacity: usize,
) -> Result<Channel<S, N, E>, ActorError> {
    channel_with_clock::<S, N, E, ()>(capacity)
}

#[doc(hidden)]
pub fn channel_with_clock<S, const N: usize, E, Clock>(
    capacity: usize,
) -> Result<Channel<S, N, E, Clock>, ActorError> {
    if capacity == 0 {
        return Err(ActorError::InvalidCapacity);
    }

    let inner = Rc::new(Inner {
        queue: RefCell::new(VecDeque::new()),
        services: RefCell::new(VecDeque::new()),
        prefer_service: Cell::new(false),
        group: RefCell::new(None),
        sessions: RefCell::new(alloc::vec::Vec::new()),
        prune_at: Cell::new(64),
        open: Cell::new(true),
        handles: Cell::new(1),
        capacity,
        closed: Event::default(),
        completion: Rc::new(Completed {
            ready: RefCell::new(None),
            result: RefCell::new(None),
            diagnostics: Rc::new(RefCell::new(alloc::vec::Vec::new())),
            changed: Event::default(),
        }),
    });

    Ok((
        Handle {
            inner: inner.clone(),
            role: PhantomData,
        },
        Owner {
            inner,
            hooks: hooks::AktorHooks::default(),
        },
    ))
}

impl<S, const N: usize, E> Inner<S, N, E> {
    pub fn close(&self) {
        if self.open.replace(false) {
            self.closed.notify();
            self.wake_sessions();
        }
    }

    fn wake_sessions(&self) {
        let callbacks: alloc::vec::Vec<_> = {
            let mut sessions = self.sessions.borrow_mut();
            sessions.retain(|session| session.strong_count() != 0);
            sessions.iter().filter_map(Weak::upgrade).collect()
        };

        for callback in callbacks {
            callback();
        }
    }

    pub fn register_session(&self, callback: &Rc<dyn Fn() -> bool>) {
        let mut sessions = self.sessions.borrow_mut();

        while sessions
            .last()
            .is_some_and(|session| session.strong_count() == 0)
        {
            sessions.pop();
        }

        if sessions.len() >= self.prune_at.get() {
            sessions.retain(|session| session.strong_count() != 0);
            self.prune_at.set(sessions.len().saturating_mul(2).max(64));
        }

        sessions.push(Rc::downgrade(callback));
    }

    pub fn lost(&self) -> bool {
        let supervisor = self.group.borrow().clone();

        if let Some((name, group)) = supervisor {
            group.fail(crate::ActorFailure {
                kind: None,
                actor: name,
                phase: "call".into(),
                message: "actor stopped without an output".into(),
            });

            true
        } else {
            false
        }
    }

    pub fn rejected(&self) -> bool {
        if self
            .group
            .borrow()
            .as_ref()
            .is_some_and(|(_, group)| group.is_stopping())
        {
            true
        } else {
            self.lost()
        }
    }

    pub fn service(&self, message: Message<S>) {
        self.services.borrow_mut().push_back(message);
        self.closed.notify();
    }

    pub fn enqueue(&self, message: Message<S>) -> Result<(), Message<S>> {
        let mut queue = self.queue.borrow_mut();

        if queue.len() == self.capacity {
            return Err(message);
        }

        queue.push_back(message);
        drop(queue);
        self.closed.notify();
        Ok(())
    }

    pub fn fail(&self, error: &OwnerError<E>) {
        let supervisor = self.group.borrow().clone();

        if let Some((name, group)) = supervisor
            && !(matches!(error, OwnerError::Cancelled) && group.is_stopping())
        {
            group.fail(crate::ActorFailure {
                kind: None,
                actor: name,
                phase: match error {
                    OwnerError::Setup(_) => "setup",
                    OwnerError::Cleanup(_) => "cleanup",
                    OwnerError::Runner(_) => "runner",
                    OwnerError::Cancelled => "owner",
                }
                .into(),
                message: alloc::format!("{error}"),
            });
        }
    }

    pub fn finish(&self, result: Result<(), Rc<OwnerError<E>>>) {
        if let Err(error) = &result {
            self.fail(error);
        }

        self.close();

        if self.completion.result.borrow().is_some() {
            return;
        }

        let mut completion = CompletionGuard {
            inner: self,
            failure: result.as_ref().err().cloned().unwrap_or_else(|| {
                Rc::new(OwnerError::Runner(crate::AktorError::new(
                    "actor work destruction failed",
                )))
            }),
            armed: true,
        };

        let queued = core::mem::take(&mut *self.queue.borrow_mut());
        let services = core::mem::take(&mut *self.services.borrow_mut());
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let primary = crate::local::panic::discard(queued.into_iter().chain(services), |payload| {
            self.completion
                .diagnostics
                .borrow_mut()
                .push(crate::AktorError::new(
                    crate::group::shutdown::panic_message(payload),
                ));
        });
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        for message in queued.into_iter().chain(services) {
            drop(message);
        }
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        let result = if result.is_ok() && primary.is_some() {
            Err(completion.failure.clone())
        } else {
            result
        };

        self.complete(result);
        completion.armed = false;
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }
    }

    fn complete(&self, result: Result<(), Rc<OwnerError<E>>>) {
        if self.completion.result.borrow().is_some() {
            return;
        }

        if self.completion.ready.borrow().is_none() {
            *self.completion.ready.borrow_mut() = Some(result.clone());
        }

        *self.completion.result.borrow_mut() = Some(result);
        self.completion.changed.notify();
        self.wake_sessions();
    }
}

impl<S, const N: usize, E> Drop for CompletionGuard<'_, S, N, E> {
    fn drop(&mut self) {
        if self.armed {
            self.inner.complete(Err(self.failure.clone()));
        }
    }
}

impl<S, const N: usize, E, Role, Clock> Handle<S, N, E, Role, Clock> {
    #[doc(hidden)]
    pub async fn wait_closing(&self) {
        let changed = self.inner.closed.listen();

        poll_fn(|cx| {
            changed.register(cx);

            if self.inner.open.get() {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "wasm_browser_workers",
        all(feature = "browser_local", target_family = "wasm")
    ))]
    #[doc(hidden)]
    pub fn manage(&self, name: alloc::string::String, kill: crate::KillSwitch) {
        *self.inner.group.borrow_mut() = Some((name, kill.into()));
    }

    pub fn downgrade(&self) -> WeakHandle<S, N, E, Role, Clock> {
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

    pub fn with_role<NewRole>(self) -> Handle<S, N, E, NewRole, Clock> {
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
impl<S, const N: usize, E, Role, Clock> Drop for Handle<S, N, E, Role, Clock> {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;

        self.inner.handles.set(handles);

        if handles == 0 {
            self.inner.close();
        }
    }
}

impl<S, const N: usize, E, Role, Clock> WeakHandle<S, N, E, Role, Clock> {
    pub fn upgrade(&self) -> Option<Handle<S, N, E, Role, Clock>> {
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
impl<S, const N: usize, E, Role, Clock> Clone for WeakHandle<S, N, E, Role, Clock> {
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
        self.run_with_custom(setup, cleanup, runner::serve).await
    }

    /// Each operation completes before the next starts. Shutdown also awaits cleanup.
    pub async fn run<Cleanup, CleanupFuture>(
        self,
        state: S,
        cleanup: Cleanup,
    ) -> Result<(), Rc<OwnerError<E>>>
    where
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), crate::AktorCleanupError<E>>>,
    {
        self.run_custom(state, cleanup, runner::serve).await
    }

    async fn dispose_hooks(&mut self) {
        let hooks = core::mem::take(&mut self.hooks);
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        {
            let report = |payload: &Box<dyn std::any::Any + Send>| {
                self.inner
                    .completion
                    .diagnostics
                    .borrow_mut()
                    .push(crate::AktorError::new(
                        crate::group::shutdown::panic_message(payload),
                    ));
            };

            let primary = crate::local::panic::discard_hooks(hooks, report);

            if let Some(payload) = primary {
                std::panic::resume_unwind(payload);
            }
        }
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        drop(hooks);
    }
}
impl<S, const N: usize, E> Drop for Owner<S, N, E> {
    fn drop(&mut self) {
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        ))]
        {
            let report = |payload: &Box<dyn std::any::Any + Send>| {
                let error = crate::AktorError::new(crate::group::shutdown::panic_message(payload));
                self.inner.fail(&OwnerError::Runner(error.clone()));
                self.inner.completion.diagnostics.borrow_mut().push(error);
            };

            let hooks = core::mem::take(&mut self.hooks);
            let mut primary = crate::local::panic::discard_hooks(hooks, report);

            if let Some(payload) = &primary {
                report(payload);
            }

            let unfinished = self.inner.completion.result.borrow().is_none();

            if unfinished
                && let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.inner.finish(Err(Rc::new(OwnerError::Cancelled)));
                }))
            {
                report(&payload);

                if primary.is_none() {
                    primary = Some(payload);
                } else {
                    crate::listener::failure::dispose_secondary(payload);
                }
            }

            if let Some(payload) = primary {
                if std::thread::panicking() {
                    crate::listener::failure::dispose_secondary(payload);
                } else {
                    std::panic::resume_unwind(payload);
                }
            }
        }
        #[cfg(not(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm"))
        )))]
        if self.inner.completion.result.borrow().is_none() {
            self.inner.finish(Err(Rc::new(OwnerError::Cancelled)));
        }
    }
}

impl<E> Completion<E> {
    #[doc(hidden)]
    pub fn diagnostics(&self) -> alloc::vec::Vec<crate::AktorError> {
        self.inner.diagnostics.borrow().clone()
    }

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
            Self::Runner(error) => write!(f, "actor runner failed: {error}"),
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
            Self::Runner(error) => Some(error),
            Self::Cancelled => None,
        }
    }
}
