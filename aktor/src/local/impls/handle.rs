use super::super::*;
use core::future::poll_fn;

impl<S, Role, Clock> Handle<S, Role, Clock> {
    /// Stop serving calls, `end` still runs. Shutdown does this once an actor's grace is up.
    #[doc(hidden)]
    pub fn cancel_calls(&self) {
        self.inner.cancelled.set(true);
        self.inner.closed.notify();
    }

    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub async fn run<F: IntoFuture>(&self, work: F) -> Option<F::Output> {
        let work = work.into_future();

        let signal = self
            .inner
            .group
            .borrow()
            .as_ref()
            .map(|(_, _, signal)| signal.clone());

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
        self.manage_on(name, crate::AktorExecution::Local, kill, false);
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
        kind: crate::AktorExecution,
        kill: crate::AktorKillSwitch,
        standard: bool,
    ) {
        *self.inner.group.borrow_mut() =
            Some((name.into(), kind, StopSignal::Native { kill, standard }));
    }

    /// The registered launch attempt this handle belongs to.
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        self.inner
            .group
            .borrow()
            .as_ref()
            .and_then(|(_, _, signal)| signal.actor_id())
    }

    /// Use the group's completion to wait for the shutdown report or confirm that every actor
    /// has stopped. Returns `None` if the actor is not in a native/browser `AktorGroup`.
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm")),
        feature = "wasm_browser_workers",
        all(feature = "browser_local", target_family = "wasm")
    ))]
    pub fn group_completion(&self) -> Option<crate::AktorGroupCompletion> {
        self.inner
            .group
            .borrow()
            .as_ref()
            .and_then(|(_, _, signal)| match signal {
                StopSignal::Native { kill, .. } => Some(kill.completion()),
                _ => None,
            })
    }

    pub fn downgrade(&self) -> WeakHandle<S, Role, Clock> {
        WeakHandle {
            inner: Rc::downgrade(&self.inner),
            role: PhantomData,
        }
    }

    /// Wait for setup, returning a diagnostic on failure.
    pub async fn ready(&self) -> Result<(), (OwnerError, String)> {
        match self.observe_ready().await {
            Ok(()) => Ok(()),
            Err((error, report)) => {
                self.completion().receive();
                Err((error.without_data(), report))
            }
        }
    }

    pub(crate) async fn observe_ready(&self) -> Result<(), (SharedOwnerError<()>, String)> {
        let changed = self.inner.completion.changed.listen();

        poll_fn(|context| {
            changed.register(context);

            match self.inner.completion.ready.borrow().clone() {
                Some(Ok(())) => Poll::Ready(Ok(())),
                Some(Err(error)) => Poll::Ready(Err(SharedOwnerError::from_failure(
                    error,
                    &self.inner.completion.diagnostics.borrow(),
                ))),
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

    pub fn with_role<NewRole>(self) -> Handle<S, NewRole, Clock> {
        self.inner.handles.set(self.inner.handles.get() + 1);

        Handle {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }

    /// Start closing now, queued calls finish before cleanup.
    #[doc(hidden)]
    pub fn shutdown(&self) -> Completion<()> {
        self.inner.close();
        self.completion()
    }

    #[doc(hidden)]
    pub fn completion(&self) -> Completion<()> {
        Completion {
            inner: self.inner.completion.clone(),
            result: self.inner.completion.result.clone(),
        }
    }
}
impl<S, Role, Clock> Clone for Handle<S, Role, Clock> {
    fn clone(&self) -> Self {
        self.new_handle()
    }
}
impl<S, Role, Clock> Drop for Handle<S, Role, Clock> {
    fn drop(&mut self) {
        let handles = self.inner.handles.get() - 1;

        self.inner.handles.set(handles);

        if handles == 0 {
            self.inner.close();
        }
    }
}

impl<S, Role, Clock> WeakHandle<S, Role, Clock> {
    #[cfg(any(
        all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")),
        all(
            feature = "browser_local",
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    pub(crate) async fn wait_closing(&self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let changed = inner.closed.listen();

        poll_fn(|cx| {
            changed.register(cx);

            if inner.open.get() && inner.handles.get() > 0 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }

    #[cfg(any(
        all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")),
        all(
            feature = "browser_local",
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    pub(crate) fn shutdown(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.close();
        }
    }

    #[cfg(any(
        all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")),
        all(
            feature = "browser_local",
            target_family = "wasm",
            target_os = "unknown"
        )
    ))]
    pub(crate) fn cancel_calls(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.cancelled.set(true);
            inner.closed.notify();
        }
    }

    pub fn upgrade(&self) -> Option<Handle<S, Role, Clock>> {
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
impl<S, Role, Clock> Clone for WeakHandle<S, Role, Clock> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }
}
