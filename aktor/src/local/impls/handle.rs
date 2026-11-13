use super::super::*;
use core::future::poll_fn;

impl<S, const N: usize, E, Role, Clock> Handle<S, N, E, Role, Clock> {
    /// Stop serving calls, `end` still runs. Shutdown does this once an actor's grace is up.
    #[doc(hidden)]
    pub fn cancel_calls(&self) {
        self.inner.cancelled.set(true);
        self.inner.closed.notify();
    }

    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        let signal = self
            .inner
            .group
            .borrow()
            .as_ref()
            .map(|(_, signal)| signal.clone());

        let Some(signal) = signal else {
            return Some(work.await);
        };

        let mut stopping = core::pin::pin!(signal.wait_stopping());
        let mut work = core::pin::pin!(work);

        poll_fn(|cx| {
            if stopping.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }

            work.as_mut().poll(cx).map(Some)
        })
        .await
    }

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
    pub fn manage(&self, name: alloc::string::String, kill: crate::AktorKillSwitch) {
        self.manage_on(name, kill, false);
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "wasm_browser_workers",
        all(feature = "browser_local", target_family = "wasm")
    ))]
    pub(crate) fn manage_on(
        &self,
        name: alloc::string::String,
        kill: crate::AktorKillSwitch,
        standard: bool,
    ) {
        *self.inner.group.borrow_mut() = Some((name.into(), StopSignal::Native { kill, standard }));
    }

    pub fn downgrade(&self) -> WeakHandle<S, N, E, Role, Clock> {
        WeakHandle {
            inner: Rc::downgrade(&self.inner),
            role: PhantomData,
        }
    }

    /// Wait for setup, returning a diagnostic on failure.
    pub async fn ready(&self) -> Result<(), (OwnerError, String)> {
        self.ready_with_data()
            .await
            .map_err(|(error, report)| (error.without_data(), report))
    }

    /// Wait for setup and get the error with its application data.
    pub async fn ready_with_data(&self) -> Result<(), (SharedOwnerError<E>, String)> {
        let changed = self.inner.completion.changed.listen();

        poll_fn(|context| {
            changed.register(context);

            match self.inner.completion.ready.borrow().clone() {
                Some(result) => Poll::Ready(result.map_err(|error| {
                    SharedOwnerError::from_failure(
                        error,
                        &self.inner.completion.diagnostics.borrow(),
                    )
                })),
                None => Poll::Pending,
            }
        })
        .await
    }

    fn new_handle(&self) -> Self {
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
    #[doc(hidden)]
    pub fn shutdown(&self) -> Completion<E> {
        self.inner.close();
        self.completion()
    }

    #[doc(hidden)]
    pub fn completion(&self) -> Completion<E> {
        Completion {
            inner: self.inner.completion.clone(),
        }
    }
}
impl<S, const N: usize, E, Role, Clock> Clone for Handle<S, N, E, Role, Clock> {
    fn clone(&self) -> Self {
        self.new_handle()
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
