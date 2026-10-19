use super::*;
use aktor::message::call_async;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn deadlines() {
    let (handle, listener) = channel(1).unwrap();
    let mut reply = call(
        &handle,
        |state: &mut Vec<&str>, value| {
            state.push(value);
            state.len()
        },
        "accepted",
    )
    .send()
    .await;

    assert!(timeout(Duration::from_millis(1), &mut reply).await.is_err());
    assert!(
        timeout(
            Duration::from_millis(1),
            call(
                &handle,
                |state: &mut Vec<&str>, value| {
                    state.push(value);
                    state.len()
                },
                "unsent"
            )
            .send()
        )
        .await
        .is_err()
    );

    drop(handle);

    assert_eq!(listener.run(Vec::new()).await, ["accepted"]);
    assert_eq!(reply.await, 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_deadline() {
    let mut group = AktorGroup::new();
    group.start().unwrap();
    let actor = group.spawn_value("deadline", ()).await.unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    let mut reply = call_async(
        &actor.handle,
        async move |_, ()| released.await.unwrap(),
        (),
    )
    .send()
    .await;

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
    reply.await;
    group.shutdown().await;
}
