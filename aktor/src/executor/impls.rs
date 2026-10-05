use super::*;
use core::{
    future::Future,
    task::{Context, Poll, Waker},
};
use std::{io, sync::Arc, task::Wake, thread};

impl Driver {
    pub fn new(standard: bool) -> io::Result<Self> {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if standard {
            return Ok(Self::Std);
        }

        let _ = standard;

        #[cfg(feature = "tokio")]
        {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map(Self::Tokio)
        }
        #[cfg(not(feature = "tokio"))]
        Err(io::Error::other(
            "the Tokio driver requires the tokio feature",
        ))
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(runtime) => runtime.block_on(future),
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => block_on(future),
        }
    }
}

impl Spawner {
    pub fn current(standard: bool) -> Result<Self, io::Error> {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if standard {
            return Ok(Self::Std);
        }

        let _ = standard;

        #[cfg(feature = "tokio")]
        {
            tokio::runtime::Handle::try_current()
                .map(Self::Tokio)
                .map_err(io::Error::other)
        }
        #[cfg(not(feature = "tokio"))]
        Err(io::Error::other(
            "the Tokio spawner requires the tokio feature",
        ))
    }

    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> io::Result<()> {
        self.spawn_named("aktor observer", future)
    }

    pub fn spawn_named(
        &self,
        name: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        #[cfg(test)]
        let failure = FAIL_SPAWN.with(|failure| failure.get());

        #[cfg(test)]
        if failure == Some(name) {
            return Err(io::Error::other("injected scheduling failure"));
        }

        let _ = name;

        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(runtime) => {
                runtime.spawn(future);
                Ok(())
            }
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => thread::Builder::new()
                .name(name.into())
                .spawn(move || {
                    #[cfg(test)]
                    FAIL_SPAWN.with(|setting| setting.set(failure));
                    block_on(future)
                })
                .map(|_| ()),
        }
    }

    pub async fn join<S: Send + 'static>(
        &self,
        thread: crate::listener::Dedicated<S>,
    ) -> Result<S, crate::listener::DedicatedJoinError> {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(_) => thread.join_async().await,
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => {
                thread.completion().wait().await;
                thread.join()
            }
        }
    }
}

#[cfg(test)]
thread_local! {
    pub static FAIL_SPAWN: core::cell::Cell<Option<&'static str>> = const { core::cell::Cell::new(None) };
}

struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

thread_local! {
    static THREAD_WAKER: Waker = Waker::from(Arc::new(ThreadWake(thread::current())));
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    let waker = THREAD_WAKER
        .try_with(Waker::clone)
        .unwrap_or_else(|_| Waker::from(Arc::new(ThreadWake(thread::current()))));
    let mut context = Context::from_waker(&waker);
    let mut future = core::pin::pin!(future);

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}

#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
pub async fn sleep_until(deadline: std::time::Instant) {
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());

        if remaining.is_zero() {
            return;
        }

        futures_timer::Delay::new(remaining.min(std::time::Duration::from_secs(86_400))).await;
    }
}
