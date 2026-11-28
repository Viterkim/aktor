use super::*;
use core::{
    future::Future,
    task::{Context, Poll, Waker},
};
#[cfg(test)]
use std::io;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Wake,
    thread,
};

impl Driver {
    pub fn new(standard: bool) -> Result<Self, (DriverError, String)> {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if standard {
            return Ok(Self::Std);
        }

        let _ = standard;

        #[cfg(feature = "tokio")]
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .er(|| DriverError::Runtime)?;

            Ok(Self::Tokio(runtime))
        }
        #[cfg(not(feature = "tokio"))]
        er_bail!(DriverError::Disabled)
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

#[cfg(not(target_family = "wasm"))]
#[derive(Er)]
pub(crate) struct CurrentSpawnerErr;

#[cfg(not(target_family = "wasm"))]
#[derive(Er)]
pub(crate) struct SpawnErr {
    name: &'static str,
}
impl Spawner {
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn current(standard: bool) -> ErResult<Self, CurrentSpawnerErr> {
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        if standard {
            return Ok(Self::Std);
        }

        let _ = standard;

        #[cfg(feature = "tokio")]
        {
            tokio::runtime::Handle::try_current()
                .map(Self::Tokio)
                .er(())
        }
        #[cfg(not(feature = "tokio"))]
        er_bail!(
            CurrentSpawnerErr,
            "the Tokio spawner requires the tokio feature"
        )
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn spawn_named(
        &self,
        name: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> ErResult<(), SpawnErr> {
        #[cfg(test)]
        let failure = check_spawn(name).er(name)?;

        let _ = name;

        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(runtime) => {
                #[cfg(not(test))]
                runtime.spawn(future);
                #[cfg(test)]
                runtime.spawn(async move {
                    FAIL_SPAWN.with(|setting| setting.set(failure));
                    future.await;
                });

                Ok(())
            }
            #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
            Self::Std => {
                // Futures share the thread, each one keeps its own injected failure.
                #[cfg(test)]
                let future = {
                    let mut future = Box::pin(future);

                    core::future::poll_fn(move |cx| {
                        let previous = FAIL_SPAWN.with(|setting| setting.replace(failure));
                        let polled = future.as_mut().poll(cx);

                        FAIL_SPAWN.with(|setting| setting.set(previous));
                        polled
                    })
                };

                super::shared::spawn(Box::pin(future)).er(name)
            }
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
pub fn check_spawn(name: &str) -> io::Result<Option<&'static str>> {
    let failure = FAIL_SPAWN.with(|failure| failure.get());

    if failure == Some(name)
        && FAIL_SPAWN_AFTER.with(|remaining| {
            let count = remaining.get();
            remaining.set(count.saturating_sub(1));
            count == 0
        })
    {
        return Err(io::Error::other("injected scheduling failure"));
    }

    Ok(failure)
}

#[cfg(test)]
thread_local! {
    pub static FAIL_SPAWN: core::cell::Cell<Option<&'static str>> = const { core::cell::Cell::new(None) };
    pub static FAIL_SPAWN_AFTER: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}

struct ThreadWake {
    thread: thread::Thread,
    notified: AtomicBool,
}
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.notified.swap(true, Ordering::Release) {
            self.thread.unpark();
        }
    }
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    // The park token belongs to the whole thread. The future can eat it with a park of its own,
    // and so can a nested block_on, so each call keeps its own flag for the wake.
    let wake = Arc::new(ThreadWake {
        thread: thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut future = core::pin::pin!(future);

    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }

        while !wake.notified.swap(false, Ordering::Acquire) {
            thread::park();
        }
    }
}

#[cfg(not(target_family = "wasm"))]
pub async fn sleep_until(deadline: std::time::Instant) {
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());

        if remaining.is_zero() {
            return;
        }

        futures_timer::Delay::new(remaining.min(std::time::Duration::from_secs(86_400))).await;
    }
}
