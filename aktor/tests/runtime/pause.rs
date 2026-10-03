use super::*;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

fn add(handle: &Handle<usize>, by: usize) -> Request<'_, usize, usize> {
    call(
        handle,
        |state, by| {
            *state += by;
            *state
        },
        by,
    )
}

#[tokio::test]
async fn resume() {
    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let records = cleaned.clone();
    let (started, cleaning) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut gate = Some((started, released));

    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "restart".into(),
        capacity: 1,
        failure: FailurePolicy::Abort,
        setup: || Ok::<_, &'static str>(0usize),
        cleanup: move |state| {
            records.lock().unwrap().push(state);

            if let Some((started, released)) = gate.take() {
                started.send(()).unwrap();
                released.recv().unwrap();
            }

            Ok::<_, std::io::Error>(())
        },
    })
    .await
    .unwrap();

    let other = handle.new_handle();
    let controller = actor.new_controller();

    assert_eq!(add(&handle, 1).await, 1);
    assert!(matches!(
        actor.resume(|| Ok(9)).await,
        Err(LifecycleError::AlreadyRunning)
    ));

    let mut before = Box::pin(add(&handle, 4));
    assert!(poll(before.as_mut()).is_pending());

    let mut pause = Box::pin(actor.pause());
    assert!(poll(pause.as_mut()).is_pending());
    cleaning.await.unwrap();
    release.send(()).unwrap();
    pause.await.unwrap();

    assert_eq!(before.await, 5);
    assert!(!actor.is_running());

    let mut parked = Box::pin(add(&handle, 1));
    assert!(poll(parked.as_mut()).is_pending());
    assert!(
        add(&other, 3)
            .timeout(Duration::from_millis(1))
            .await
            .is_err()
    );

    assert!(matches!(
        actor.resume(|| Err("not yet")).await,
        Err(LifecycleError::Failed("not yet"))
    ));
    assert!(poll(parked.as_mut()).is_pending());

    controller.resume(|| Ok(100)).await.unwrap();
    assert_eq!(parked.await, 101);
    assert_eq!(add(&other, 3).await, 104);

    actor.shutdown().await.unwrap();
    thread.join_async().await.unwrap().unwrap();

    assert_eq!(*cleaned.lock().unwrap(), [5, 104]);
}
