use super::*;
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
