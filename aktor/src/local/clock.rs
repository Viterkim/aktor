#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
use crate::{Timeout, timeout::WaitStatus};
#[cfg(any(
    feature = "embassy",
    all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    )
))]
use alloc::boxed::Box;
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
use core::time::Duration;

#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm")),
    feature = "embassy",
    feature = "browser_local"
))]
#[doc(hidden)]
pub trait AktorClock {
    fn timeout<F>(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Timeout<F>;
}

#[cfg(feature = "tokio")]
pub struct Tokio;
#[cfg(feature = "tokio")]
impl AktorClock for Tokio {
    fn timeout<F>(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Timeout<F> {
        Timeout::native(future, duration, status)
    }
}

#[cfg(feature = "embassy")]
pub struct Embassy;
#[cfg(feature = "embassy")]
impl AktorClock for Embassy {
    fn timeout<F>(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Timeout<F> {
        Timeout::local(future, duration, status)
    }
}

#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
pub struct Browser;
#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
impl AktorClock for Browser {
    fn timeout<F>(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Timeout<F> {
        Timeout::browser(future, duration, status)
    }
}

#[doc(hidden)]
pub trait AktorGroupClock: 'static {
    type Deadline: Copy;
    const EXECUTION: crate::AktorExecution = crate::AktorExecution::Local;

    fn deadline(duration: core::time::Duration) -> Self::Deadline;
    fn wait(deadline: Self::Deadline) -> crate::message::LocalFuture<'static, ()>;
}

#[cfg(feature = "embassy")]
impl AktorGroupClock for Embassy {
    type Deadline = embassy_time::Instant;
    const EXECUTION: crate::AktorExecution = crate::AktorExecution::EmbassyLocal;

    fn deadline(duration: core::time::Duration) -> Self::Deadline {
        crate::timeout::embassy_deadline(embassy_time::Instant::now(), duration)
    }

    fn wait(deadline: Self::Deadline) -> crate::message::LocalFuture<'static, ()> {
        Box::pin(embassy_time::Timer::at(deadline))
    }
}

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
impl AktorGroupClock for Tokio {
    type Deadline = std::time::Instant;

    fn deadline(duration: core::time::Duration) -> Self::Deadline {
        crate::group::shutdown_deadline(tokio::time::Instant::now().into_std(), duration)
    }

    fn wait(deadline: Self::Deadline) -> crate::message::LocalFuture<'static, ()> {
        Box::pin(tokio::time::sleep_until(deadline.into()))
    }
}

#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
impl AktorGroupClock for Browser {
    type Deadline = crate::group::Instant;

    fn deadline(duration: core::time::Duration) -> Self::Deadline {
        crate::group::shutdown_deadline(crate::group::Instant::now(), duration)
    }

    fn wait(deadline: Self::Deadline) -> crate::message::LocalFuture<'static, ()> {
        alloc::boxed::Box::pin(async move {
            let _elapsed =
                crate::group::shutdown::bounded(deadline, core::future::pending::<()>()).await;
        })
    }
}

#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
pub struct Std;
#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
impl AktorClock for Std {
    fn timeout<F>(future: F, duration: Duration, status: fn(&F) -> WaitStatus) -> Timeout<F> {
        Timeout::standard(future, duration, status)
    }
}
#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
impl AktorGroupClock for Std {
    type Deadline = std::time::Instant;

    fn deadline(duration: Duration) -> Self::Deadline {
        crate::group::shutdown_deadline(std::time::Instant::now(), duration)
    }

    fn wait(deadline: Self::Deadline) -> crate::message::LocalFuture<'static, ()> {
        Box::pin(crate::executor::sleep_until(deadline))
    }
}
