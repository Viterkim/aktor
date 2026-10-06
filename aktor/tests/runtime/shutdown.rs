use super::*;
use tokio::sync::oneshot;

#[tokio::test]
async fn cutoff() {
    let (cleaning, cleaned) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut gate = Some((cleaning, released));

    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "cutoff".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, std::convert::Infallible>(()),
        cleanup: move |_| {
            let (cleaning, released) = gate.take().unwrap();

            cleaning.send(()).unwrap();
            released.recv().unwrap();
            Ok::<_, std::convert::Infallible>(())
        },
    })
    .await
    .unwrap();

    let observer = actor.new_controller();
    let stopping = tokio::spawn(async move { actor.shutdown().await });

    cleaned.await.unwrap();

    let running = observer.is_running();
    let closed = matches!(
        call(&handle, |_, ()| (), ()).try_cast(),
        Err(TrySendError::Closed(_))
    );

    release.send(()).unwrap();
    stopping.await.unwrap().unwrap();
    thread.join_async().await.unwrap().unwrap();

    assert!(!running);
    assert!(closed, "fresh admission succeeded during permanent cleanup");
}

#[tokio::test]
async fn waiting_admission() {
    let (entered, running) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();

    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "waiting admission".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, std::convert::Infallible>(0usize),
        cleanup: |state| {
            assert_eq!(state, 2);
            Ok::<_, std::convert::Infallible>(())
        },
    })
    .await
    .unwrap();

    let first = call(
        &handle,
        move |state, ()| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            *state += 1;
        },
        (),
    )
    .send()
    .await;

    running.await.unwrap();

    let second = call(&handle, |state, ()| *state += 1, ()).send().await;

    let mut waiting = Box::pin(call(&handle, |state, ()| *state += 100, ()));

    assert!(poll(waiting.as_mut()).is_pending());

    let mut stopping = Box::pin(actor.shutdown());

    assert!(poll(stopping.as_mut()).is_pending());

    release.send(()).unwrap();
    stopping.await.unwrap();

    first.await;
    second.await;
    assert!(AssertUnwindSafe(waiting).catch_unwind().await.is_err());
    thread.join_async().await.unwrap().unwrap();
}

#[tokio::test]
async fn paused() {
    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "shutdown".into(),
        capacity: 1,
        failure: FailurePolicy::Abort,
        setup: || Ok::<_, &'static str>(()),
        cleanup: |_| Ok::<_, std::io::Error>(()),
    })
    .await
    .unwrap();

    actor.pause().await.unwrap();
    assert!(
        call(&handle, |_: &mut (), ()| panic!("paused work ran"), ())
            .timeout(Duration::from_millis(1))
            .await
            .is_err()
    );

    actor.shutdown().await.unwrap();
    thread.join_async().await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_cleanup() {
    let (entered, cleaning) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut gate = Some((entered, released));

    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "failed cleanup".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, std::convert::Infallible>(()),
        cleanup: move |_| {
            if let Some((entered, released)) = gate.take() {
                let _ = entered.send(());
                let _ = released.recv();
            }

            Ok::<_, std::convert::Infallible>(())
        },
    })
    .await
    .unwrap();

    call(&handle, |_, ()| -> () { panic!("operation failed") }, ())
        .cast()
        .await;
    cleaning.await.unwrap();

    assert!(!actor.is_running());
    assert!(call(&handle, |_, ()| (), ()).try_send().is_err());

    release.send(()).unwrap();
    assert!(thread.join_async().await.is_err());
}
