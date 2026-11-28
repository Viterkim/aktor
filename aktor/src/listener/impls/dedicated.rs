use super::super::*;
#[cfg(feature = "tokio")]
use tokio::task;

impl<T: Send + 'static> OwnerThread<T> {
    #[cfg(not(target_family = "wasm"))]
    pub fn new(thread: thread::JoinHandle<T>, life: Option<ThreadLife>) -> Self {
        Self {
            thread: Some(thread),
            life,
            reap: |thread, life| {
                // An owner that already ended joins right away, its entry goes after the join.
                if thread.is_finished() {
                    let joined = thread.join();

                    // A panic payload or the state can panic as it drops, and this can run while
                    // the owner of this handle is already unwinding.
                    let dropped =
                        std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| drop(joined)));

                    if let Err(payload) = dropped {
                        crate::panic::dispose_secondary(payload);
                    }
                    return;
                }

                // When the reaper can't start, its closure drops without releasing the entry.
                // The group keeps counting a thread that still runs and nobody can join.
                let life = core::mem::ManuallyDrop::new(life);
                let _reaper = thread::Builder::new()
                    .name("aktor join".into())
                    .spawn(move || {
                        // Locals drop in reverse, the join's result goes before the entry.
                        let _life = core::mem::ManuallyDrop::into_inner(life);
                        let _joined = thread.join();
                    });
            },
        }
    }
}
impl<T> OwnerThread<T> {
    pub fn join(mut self) -> thread::Result<T> {
        let Some(thread) = self.thread.take() else {
            return Err(Box::new("actor thread was already joined"));
        };
        let joined = thread.join();

        self.life = None;
        joined
    }

    pub fn is_finished(&self) -> bool {
        self.thread
            .as_ref()
            .is_some_and(thread::JoinHandle::is_finished)
    }
}
impl<T> Drop for OwnerThread<T> {
    fn drop(&mut self) {
        // Nobody waited for the join. The group keeps counting the thread until it's really gone.
        if let (Some(thread), Some(life)) = (self.thread.take(), self.life.take()) {
            (self.reap)(thread, life);
        }
    }
}

#[cfg(not(target_family = "wasm"))]
impl<S: Send + 'static> Dedicated<S> {
    /// Starts an owner thread. The group counts it as running until the OS join returns.
    pub fn spawn(
        name: String,
        life: Option<ThreadLife>,
        finished: watch::Receiver<bool>,
        owner: impl FnOnce() -> Option<S> + Send + 'static,
    ) -> std::io::Result<Self> {
        #[cfg(test)]
        if crate::executor::FAIL_SPAWN.with(|setting| setting.get()) == Some("aktor owner") {
            return Err(std::io::Error::other("injected owner thread failure"));
        }

        let name = if name.contains('\0') {
            name.replace('\0', "�")
        } else {
            name
        };
        let thread = thread::Builder::new().name(name).spawn(owner)?;

        // A guard inside the thread would let go before its thread local destructors ran.
        Ok(Self {
            thread: OwnerThread::new(thread, life),
            finished,
        })
    }
}

impl<S> Dedicated<S> {
    pub fn completion(&self) -> CompletionObserver {
        CompletionObserver {
            finished: self.finished.clone(),
        }
    }

    pub fn join(self) -> Result<S, DedicatedJoinError> {
        match self.thread.join() {
            Ok(Some(state)) => Ok(state),
            Err(payload) => Err(DedicatedJoinError {
                physically_joined: true,
                payload: parking_lot::Mutex::new(payload),
            }),
            Ok(None) => Err(DedicatedJoinError {
                physically_joined: true,
                payload: parking_lot::Mutex::new(Box::new("actor thread returned no output")),
            }),
        }
    }

    /// Join without blocking the async runtime.
    #[cfg(feature = "tokio")]
    pub async fn join_async(self) -> Result<S, DedicatedJoinError>
    where
        S: Send + 'static,
    {
        self.completion().wait().await;

        match task::spawn_blocking(move || self.join()).await {
            Ok(joined) => joined,
            Err(error) => Err(DedicatedJoinError {
                physically_joined: false,
                payload: parking_lot::Mutex::new(if error.is_panic() {
                    error.into_panic()
                } else {
                    Box::new(error)
                }),
            }),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }
}

impl CompletionObserver {
    pub async fn wait(&mut self) {
        let _result = self.finished.changed().await;
    }

    pub fn is_complete(&self) -> bool {
        self.finished.has_changed().is_err()
    }
}
