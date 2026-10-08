use super::super::*;
use core::future::poll_fn;

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

    /// Wait for setup, or get an owned diagnostic without retaining setup data.
    pub async fn ready(&self) -> Result<(), OwnerError> {
        self.ready_with_data()
            .await
            .map_err(|error| error.error.report())
    }

    /// Observe the original setup error and its data on this executor.
    pub async fn ready_with_data(&self) -> Result<(), SharedOwnerError<E>> {
        let changed = self.inner.completion.changed.listen();

        poll_fn(|context| {
            changed.register(context);

            match self.inner.completion.ready.borrow().clone() {
                Some(result) => Poll::Ready(result.map_err(|error| SharedOwnerError { error })),
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
