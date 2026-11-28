#![cfg(feature = "testing")]
use super::*;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn deadlines() {
    let (actor, _group) = thread_actor(Vec::new(), 1).await;
    let release = hold(&actor).await;
    let mut reply = call(&actor, async |state| {
        state.push("accepted");
        state.len()
    })
    .send()
    .await
    .unwrap();

    assert!(timeout(Duration::from_millis(1), &mut reply).await.is_err());
    assert!(
        timeout(
            Duration::from_millis(1),
            call(&actor, async |s| s.push("unsent")).send()
        )
        .await
        .is_err()
    );

    release.send(()).unwrap();
    assert_eq!(
        call(&actor, async |s| s.clone()).await,
        Ok(vec!["accepted"])
    );
    assert_eq!(reply.await, Ok(1));
}

#[tokio::test(start_paused = true)]
async fn shutdown_deadline() {
    let (actor, group) = thread_actor((), 1).await;
    let (release, released) = tokio::sync::oneshot::channel();
    let mut reply = call(&actor, async move |_| released.await.unwrap())
        .send()
        .await
        .unwrap();

    let started = tokio::time::Instant::now();
    let mut waiting = Box::pin(reply.timeout(Duration::from_secs(1)));
    assert!(poll(waiting.as_mut()).is_pending());

    tokio::time::advance(Duration::from_millis(500)).await;
    group.killswitch().stop();
    timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap_err();

    assert_eq!(started.elapsed(), Duration::from_secs(1));

    release.send(()).unwrap();
    reply.await.unwrap();
    let _ = group.stop().await;
}
