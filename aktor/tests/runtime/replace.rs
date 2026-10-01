use super::*;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[tokio::test]
async fn cancel() {
    let (cleaning, cleaned) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut gate = Some((cleaning, released));
    let values = Arc::new(Mutex::new(Vec::new()));
    let record = values.clone();

    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "replace".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, &'static str>(1usize),
        cleanup: move |state| {
            record.lock().unwrap().push(state);

            if let Some((cleaning, released)) = gate.take() {
                cleaning.send(()).unwrap();
                released.recv().unwrap();
            }

            Ok::<_, &'static str>(())
        },
    })
    .await
    .unwrap();

    let mut replacing = Box::pin(actor.replace(|| Ok(10)));
    assert!(poll(replacing.as_mut()).is_pending());
    cleaned.await.unwrap();
    drop(replacing);

    assert_eq!(
        call(&handle, |s, ()| *s, ()).checked().await,
        Err(CallError::NotAdmitted)
    );

    release.send(()).unwrap();
    actor.wait_running().await.unwrap();

    let reply = call(
        &handle,
        |s, ()| {
            *s += 1;
            *s
        },
        (),
    )
    .send()
    .await;
    assert_eq!(reply.await, 11);

    assert!(matches!(
        actor.replace(|| Err("setup failed")).await,
        Err(ReplaceError::Setup("setup failed"))
    ));
    assert!(!actor.is_running());
    assert!(actor.take_abandoned_setup().is_empty());
    assert_eq!(
        call(&handle, |state, ()| *state, ()).checked().await,
        Err(CallError::NotAdmitted)
    );

    actor.replace(|| Ok(20)).await.unwrap();
    assert_eq!(call(&handle, |state, ()| *state, ()).await, 20);

    actor.shutdown().await.unwrap();
    thread.join_async().await.unwrap().unwrap();
    assert_eq!(*values.lock().unwrap(), [1, 11, 20]);
}

#[tokio::test]
async fn error() {
    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "replace error".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, &'static str>(1usize),
        cleanup: |state| {
            if state == 1 {
                Err("cleanup failed")
            } else {
                Ok(())
            }
        },
    })
    .await
    .unwrap();

    assert!(
        matches!(actor.replace(|| panic!("setup must not run")).await, Err(ReplaceError::Cleanup(error)) if *error == "cleanup failed")
    );
    assert!(!actor.is_running());
    assert_eq!(
        call(&handle, |state, ()| *state, ()).checked().await,
        Err(CallError::NotAdmitted)
    );

    actor.replace(|| Ok(2)).await.unwrap();
    assert_eq!(call(&handle, |state, ()| *state, ()).await, 2);

    actor.shutdown().await.unwrap();
    let errors = thread.join_async().await.unwrap().unwrap_err();
    assert_eq!(*errors.errors[0], "cleanup failed");
}

#[tokio::test]
async fn abandoned_setup() {
    let (handle, actor, thread) = spawn(SpawnArgs {
        name: "abandoned setup".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok::<_, &'static str>(1usize),
        cleanup: |_| Ok::<_, &'static str>(()),
    })
    .await
    .unwrap();

    actor.pause().await.unwrap();

    let (entered, entering) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut resume = Box::pin(actor.resume(move || {
        entered.send(()).unwrap();
        released.recv().unwrap();
        Err("resume setup failed")
    }));
    assert!(poll(resume.as_mut()).is_pending());
    entering.await.unwrap();
    drop(resume);
    release.send(()).unwrap();
    actor.resume(|| Ok(2)).await.unwrap();

    let (entered, entering) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut replace = Box::pin(actor.replace(move || {
        entered.send(()).unwrap();
        released.recv().unwrap();
        Err("replace setup failed")
    }));
    assert!(poll(replace.as_mut()).is_pending());
    entering.await.unwrap();
    drop(replace);
    release.send(()).unwrap();
    actor.replace(|| Ok(3)).await.unwrap();

    actor.pause().await.unwrap();

    let (release, gate) = oneshot::channel();
    let mut resume = Box::pin(actor.resume_async(move || async move {
        gate.await.unwrap();
        Err("buffered resume error")
    }));
    assert!(poll(resume.as_mut()).is_pending());
    release.send(()).unwrap();
    actor.pause().await.unwrap();
    drop(resume);

    let (release, gate) = oneshot::channel();
    let mut replace = Box::pin(actor.replace_async(move || async move {
        gate.await.unwrap();
        Err("buffered replace error")
    }));
    assert!(poll(replace.as_mut()).is_pending());
    release.send(()).unwrap();
    actor.pause().await.unwrap();
    drop(replace);

    actor.resume(|| Ok(3)).await.unwrap();

    let failures = actor.take_abandoned_setup();
    assert!(matches!(
        failures.as_slice(),
        [
            AbandonedSetup::Resume("resume setup failed"),
            AbandonedSetup::Replace("replace setup failed"),
            AbandonedSetup::Resume("buffered resume error"),
            AbandonedSetup::Replace("buffered replace error")
        ]
    ));
    assert!(actor.take_abandoned_setup().is_empty());
    assert_eq!(call(&handle, |state, ()| *state, ()).await, 3);

    actor.shutdown().await.unwrap();
    thread.join_async().await.unwrap().unwrap();
}
