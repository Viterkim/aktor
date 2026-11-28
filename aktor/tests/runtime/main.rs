#[cfg(feature = "testing")]
use aktor::testing::call;
use aktor::*;

#[path = "../support/mod.rs"]
pub mod support;

use futures_util::FutureExt;
use std::time::Duration;
#[cfg(any(feature = "testing", feature = "macros", feature = "local"))]
use support::poll;

#[cfg(any(feature = "testing", feature = "local"))]
mod admission;
mod deadlines;
#[cfg(feature = "macros")]
mod latest;
mod loop_actor;
mod outcome;
#[cfg(any(feature = "testing", all(feature = "macros", feature = "local")))]
mod ownership;
mod retirement;
#[cfg(all(feature = "macros", not(target_family = "wasm")))]
mod setup;
#[cfg(feature = "macros")]
mod spawned;

#[cfg(feature = "macros")]
mod task;

#[cfg(all(feature = "macros", feature = "local", feature = "embassy_cross_core"))]
mod teardown;

#[cfg(any(feature = "macros", feature = "testing"))]
async fn thread_actor<S: Send + 'static>(
    state: S,
    capacity: usize,
) -> (Aktor<S>, AktorStarted<(), AktorGroup>) {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: support::thread_actor(state, capacity),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    {
        let (owner, actor) = actors.into_parts();
        (actor, owner)
    }
}

#[cfg(feature = "testing")]
async fn hold<S: 'static>(actor: &Aktor<S>) -> tokio::sync::oneshot::Sender<()> {
    let (entered, running) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    call(actor, async move |_| {
        entered.send(()).unwrap();
        gate.await.unwrap();
    })
    .cast()
    .await
    .unwrap();
    running.await.unwrap();

    release
}

#[cfg(any(feature = "testing", all(feature = "local", feature = "macros")))]
async fn panics<F: std::future::Future>(future: F) -> bool {
    let mut future = Box::pin(future);

    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(_)) => std::task::Poll::Ready(false),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(_) => std::task::Poll::Ready(true),
        }
    })
    .await
}
