use super::*;

#[tokio::test]
async fn stopping() {
    use std::sync::Arc;
    use tokio::sync::Notify;

    for mode in 0..4 {
        for cleanup_fails in [false, true] {
            let entered = Arc::new(Notify::new());
            let entering = entered.clone();
            let release = Arc::new(Notify::new());
            let released = release.clone();
            let mut group = AktorGroup::new();

            group.start().unwrap();

            let owner = group
                .spawn_async(ActorArgs::new(
                    "closing admission",
                    async || Ok::<_, AktorSetupError>(()),
                    move |_| {
                        let entered = entered.clone();
                        let release = release.clone();

                        async move {
                            entered.notify_one();
                            release.notified().await;

                            if cleanup_fails {
                                Err(AktorCleanupError::new("real cleanup failure"))
                            } else {
                                Ok(())
                            }
                        }
                    },
                ))
                .await
                .unwrap();

            group.killswitch().stop();
            entering.notified().await;

            let request = call(&owner.handle, |_, ()| panic!("rejected operation ran"), ());
            let mut waiting: std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> = match mode {
                0 => Box::pin(request),
                1 => Box::pin(async { drop(request.send().await) }),
                2 => Box::pin(request.cast()),
                _ => Box::pin(async {
                    let _result = request.timeout(Duration::ZERO).await;
                }),
            };

            assert!(poll(waiting.as_mut()).is_pending());
            drop(waiting);
            released.notify_one();

            let report = group.completion().await;

            assert_eq!(report.failed(), cleanup_fails);

            if cleanup_fails {
                assert_eq!(report.failure.unwrap().phase, "cleanup");
            }
        }
    }

    #[cfg(feature = "local")]
    local_stopping().await;
}

#[cfg(feature = "local")]
async fn local_stopping() {
    use std::sync::Arc;
    use tokio::sync::Notify;

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            for mode in 0..4 {
                for cleanup_fails in [false, true] {
                    let entered = Arc::new(Notify::new());
                    let entering = entered.clone();
                    let release = Arc::new(Notify::new());
                    let released = release.clone();
                    let actors = aktor_start(AktorSetup {
                        actors: AktorNew {
                            name: AktorName::new("local closure"),
                            role: AktorNoRole,
                            kind: AktorKind::TokioLocal(&executor),
                            closures: AktorClosures {
                                start: async || Ok(()),
                                end: Some(
                                    (async move |_| {
                                        entered.notify_one();
                                        release.notified().await;

                                        if cleanup_fails {
                                            Err(AktorCleanupError::new("real cleanup failure"))
                                        } else {
                                            Ok(())
                                        }
                                    })
                                    .into(),
                                ),
                                intervals: vec![],
                                before_each: None,
                                after_each: None,
                            },
                            options: AktorNewOptions { capacity: 32 },
                        },
                        shutdown: |_| {},
                        options: AktorOptions {
                            shutdown_grace: Duration::from_secs(5),
                        },
                    })
                    .await
                    .unwrap();

                    actors.killswitch().stop();
                    entering.notified().await;

                    let request = local::Request::new(
                        &actors.handles,
                        aktor::operation::Operation {
                            name: "rejected",
                            caller: std::panic::Location::caller(),
                        },
                        async |_: &mut (), ()| panic!("rejected call ran"),
                        (),
                    );

                    let mut waiting: std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> =
                        match mode {
                            0 => Box::pin(request),
                            1 => Box::pin(async { drop(request.send().await) }),
                            2 => Box::pin(request.cast()),
                            _ => Box::pin(async {
                                let _result = request.timeout(Duration::ZERO).await;
                            }),
                        };

                    assert!(poll(waiting.as_mut()).is_pending());
                    drop(waiting);
                    released.notify_one();

                    let report = actors.completion().await;

                    assert_eq!(report.failed(), cleanup_fails);

                    if cleanup_fails {
                        assert_eq!(report.failure.unwrap().phase, "cleanup");
                    }
                }
            }
        })
        .await;
}

#[tokio::test]
async fn retry() {
    let (handle, mut listener) = channel::<usize>(1).unwrap();

    call(&handle, |s, ()| *s += 1, ()).cast().await;

    let mut request = call(&handle, |s, ()| *s += 10, ());

    assert!(poll(&mut request).is_pending());

    let mut state = 0;

    listener.recv().await.unwrap().run(&mut state).await;
    request.cast().await;
    listener.recv().await.unwrap().run(&mut state).await;

    let mut submitted = call(&handle, |s, ()| *s += 100, ());

    assert!(poll(&mut submitted).is_pending());

    listener.close();
    submitted.cast().await;
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

            let rejected = panics(request.cast()).await;

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

        drop(listener);
        assert!(panics(request.send()).await);
    }
}

#[tokio::test]
async fn reply() {
    let (handle, mut listener) = channel::<usize>(1).unwrap();
    let first: Reply<Result<usize, String>> = call(&handle, |s, ()| Ok(*s), ()).send().await;

    let mut request = call(&handle, |s, ()| *s + 1, ());
    assert!(poll(&mut request).is_pending());

    listener.recv().await.unwrap().run(&mut 3).await;

    let reply: Reply<usize> = request.send().await;

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
            .send()
            .await;

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

#[tokio::test]
async fn reply_wake() {
    use std::{
        sync::{Arc, atomic::Ordering},
        task::{Context, Waker},
    };

    let (handle, mut listener) = channel::<usize>(1).unwrap();
    let mut reply = call(&handle, |s, ()| *s, ()).send().await;

    assert!(reply.try_take().is_none());

    let count = Arc::new(support::CountWake::default());
    let waker = Waker::from(count.clone());

    assert!(
        std::pin::Pin::new(&mut reply)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(reply.try_take().is_none());
    listener.recv().await.unwrap().run(&mut 85).await;
    assert!(
        count.0.load(Ordering::Relaxed) > 0,
        "try_take replaced the reply waker"
    );
    assert_eq!(reply.try_take(), Some(85));
    assert_eq!(reply.try_take(), None);
}
