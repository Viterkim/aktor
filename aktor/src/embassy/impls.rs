use super::*;
use core::{fmt, future::poll_fn};

/// Capacity bounds queued work. One operation can also be running.
pub fn channel<S, const N: usize, E>() -> Result<Channel<S, N, E>, ActorError> {
    if N == 0 {
        return Err(ActorError::InvalidCapacity);
    }

    let inner = Rc::new(Inner {
        queue: Queue::new(),
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
        }
    }

    fn finish(&self, result: Result<(), Rc<OwnerError<E>>>) {
        self.close();

        if self.completion.result.borrow().is_some() {
            return;
        }

        // Drop payloads outside the channel lock before publishing completion.
        while let Ok(message) = self.queue.try_receive() {
            drop(message);
        }

        if self.completion.ready.borrow().is_none() {
            *self.completion.ready.borrow_mut() = Some(result.clone());
        }
        *self.completion.result.borrow_mut() = Some(result);
        self.completion.changed.notify();
    }
}

impl<S, const N: usize, E, Role> Handle<S, N, E, Role> {
    pub fn downgrade(&self) -> WeakHandle<S, N, E, Role> {
        WeakHandle {
            inner: Rc::downgrade(&self.inner),
            role: PhantomData,
        }
    }

    /// Wait until the owning task has built its state, retaining any setup failure.
    pub async fn ready(&self) -> Result<(), Rc<OwnerError<E>>> {
        let changed = self.inner.completion.changed.listen();

        poll_fn(
            |context| match self.inner.completion.ready.borrow().clone() {
                Some(result) => Poll::Ready(result),
                None => {
                    changed.register(context);
                    Poll::Pending
                }
            },
        )
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

    /// Close admission now. The owning task drains work and runs cleanup.
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

    /// Setup is created and awaited on the application's owning task.
    pub async fn run_with<Setup, SetupFuture, Cleanup, CleanupFuture>(
        self,
        setup: Setup,
        cleanup: Cleanup,
    ) -> Result<(), Rc<OwnerError<E>>>
    where
        Setup: FnOnce() -> SetupFuture,
        SetupFuture: Future<Output = Result<S, E>>,
        Cleanup: FnOnce(S) -> CleanupFuture,
        CleanupFuture: Future<Output = Result<(), E>>,
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
        CleanupFuture: Future<Output = Result<(), E>>,
    {
        *self.inner.completion.ready.borrow_mut() = Some(Ok(()));
        self.inner.completion.changed.notify();

        let closing = self.inner.closed.listen();
        loop {
            let message = poll_fn(|context| {
                if !self.inner.open.get() && self.inner.queue.is_empty() {
                    return Poll::Ready(None);
                }

                closing.register(context);
                self.inner.queue.poll_receive(context).map(Some)
            })
            .await;

            let Some(mut message) = message else { break };
            message.job.run(&mut state).await;
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

        poll_fn(|context| match self.inner.result.borrow().clone() {
            Some(result) => Poll::Ready(result),
            None => {
                changed.register(context);
                Poll::Pending
            }
        })
        .await
    }
}

impl<E: fmt::Display> fmt::Display for OwnerError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(error) => write!(formatter, "actor setup failed: {error}"),
            Self::Cleanup(error) => write!(formatter, "actor cleanup failed: {error}"),
            Self::Cancelled => formatter.write_str("actor owner stopped before cleanup completed"),
        }
    }
}
impl<E: core::error::Error + 'static> core::error::Error for OwnerError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Setup(error) | Self::Cleanup(error) => Some(error),
            Self::Cancelled => None,
        }
    }
}
