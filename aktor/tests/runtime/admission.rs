use super::*;

#[tokio::test]
async fn retry() {
    let (handle, mut listener) = channel::<usize>(1).unwrap();
    call(&handle, |s, ()| *s += 1, ()).cast().await;

    let mut request = call(&handle, |s, ()| *s += 10, ());
    assert!(poll(&mut request).is_pending());

    let mut state = 0;
    listener.recv().await.unwrap().run(&mut state).await;
    request.try_cast().unwrap();
    listener.recv().await.unwrap().run(&mut state).await;

    let mut submitted = call(&handle, |s, ()| *s += 100, ());
    assert!(poll(&mut submitted).is_pending());

    listener.close();
    submitted.try_cast().unwrap();
    drop(handle);

    assert_eq!(listener.run(state).await, 111);
}

#[test]
fn capacity() {
    let max = tokio::sync::Semaphore::MAX_PERMITS;

    for capacity in [0, max + 1] {
        assert!(matches!(
            channel::<()>(capacity),
            Err(ActorError::InvalidCapacity)
        ));
    }

    for capacity in [1, max] {
        assert!(channel::<()>(capacity).is_ok());
    }
}

#[tokio::test]
async fn consumed() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let (handle, task) = spawn_local(&executor, (), 1).unwrap();
            let mut request = call(&handle, |_, ()| (), ());
            (&mut request).await;

            let rejected =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(request.try_cast())))
                    .is_err();

            drop(handle);
            task.await.unwrap();

            assert!(rejected, "consumed output converted successfully");
        })
        .await
}

#[tokio::test]
async fn closed() {
    for release_first in [false, true] {
        let (handle, mut listener) = channel::<()>(1).unwrap();
        call(&handle, |_, ()| (), ()).cast().await;

        let mut request = call(&handle, |_, ()| (), ());
        assert!(poll(&mut request).is_pending());

        if release_first {
            listener.recv().await.unwrap().run(&mut ()).await;
        }

        listener.close();

        let request = match request.try_send() {
            Err(TrySendError::Closed(request)) => request,
            _ => panic!("closed admission accepted a request"),
        };

        drop(listener);
        assert!(panics(request.send()).await);
    }
}

#[tokio::test]
async fn reply() {
    let (handle, mut listener) = channel::<usize>(1).unwrap();
    let first: Reply<Result<usize, String>> = call(&handle, |s, ()| Ok(*s), ()).try_send().unwrap();

    let request = match call(&handle, |s, ()| *s + 1, ()).try_send() {
        Err(TrySendError::Full(request)) => request,
        _ => panic!("mailbox should be full"),
    };

    listener.recv().await.unwrap().run(&mut 3).await;
    let reply: Reply<usize> = request.try_send().unwrap();
    drop(handle);
    listener.run(3).await;

    assert_eq!(tokio::spawn(first).await.unwrap(), Ok(3));
    assert_eq!(tokio::spawn(reply).await.unwrap(), 4);
}

#[tokio::test]
async fn owner_failure() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let (handle, listener) = channel::<()>(1).unwrap();
            drop(listener);

            assert!(
                AssertUnwindSafe(call(&handle, |_, ()| 1, ()))
                    .catch_unwind()
                    .await
                    .is_err()
            );

            let (handle, mut listener) = channel::<()>(1).unwrap();
            let reply = call(&handle, |_, ()| 1, ()).send().await;

            listener.close();
            drop(listener);

            assert!(AssertUnwindSafe(reply).catch_unwind().await.is_err());

            let (handle, task) =
                spawn_local_with_policy(&executor, (), 1, FailurePolicy::Unwind).unwrap();

            let reply = call(
                &handle,
                |_, ()| -> Result<(), &'static str> { Err("domain") },
                (),
            )
            .try_send()
            .unwrap();

            assert_eq!(reply.await, Err("domain"));

            drop(handle);
            task.await.unwrap();

            let (handle, task) =
                spawn_local_with_policy(&executor, (), 1, FailurePolicy::Unwind).unwrap();

            let reply = call(&handle, |_, ()| -> () { panic!("operation failed") }, ())
                .send()
                .await;

            assert!(AssertUnwindSafe(reply).catch_unwind().await.is_err());
            assert!(task.await.is_err());
        })
        .await
}

#[test]
fn local_executor() {
    let executor = tokio::task::LocalSet::new();
    let (handle, task) = spawn_local(&executor, (), 1).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    drop(handle);
    runtime.block_on(executor.run_until(task)).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cooperative() {
    let (handle, listener) = channel::<usize>(1024).unwrap();
    let sent = std::cell::Cell::new(0);

    let producer = async {
        for _ in 0..1024 {
            call(&handle, |state, ()| *state += 1, ()).cast().await;
            sent.set(sent.get() + 1);
        }
    };

    let (_, observed) = tokio::join!(biased; producer, async { sent.get() });
    drop(handle);
    let completed = listener.run(0).await;

    assert_eq!(completed, 1024);
    assert!(observed < completed, "producer monopolised the executor");
}
