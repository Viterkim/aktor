use super::*;
use core::num::NonZeroUsize;
use std::time::Duration;

#[cfg(feature = "testing")]
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
            let actors = aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "closing admission".into(),
                    kind: AktorKind::TokioThread,
                    start: aktor::AktorInit::Ready(async || ()),
                    end: move |_| {
                        let entered = entered.clone();
                        let release = release.clone();

                        async move {
                            entered.notify_one();
                            release.notified().await;

                            if cleanup_fails {
                                Err(((), String::from("real cleanup failure")))
                            } else {
                                Ok(())
                            }
                        }
                    },
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            actors.ready().await.unwrap();

            actors.killswitch().stop();
            entering.notified().await;

            let request = call(actors.handles(), async |_| panic!("rejected operation ran"));
            let error = match mode {
                0 => request.await,
                1 => request.send().await.map(drop),
                2 => request.cast().await,
                _ => request.timeout(Duration::from_secs(1)).await.unwrap(),
            }
            .unwrap_err();
            assert_eq!(error.outcome, AktorDeliveryOutcome::NotAdmitted);
            released.notify_one();

            let report = actors.completion().await.unwrap();

            assert_eq!(report.failed(), cleanup_fails);

            if cleanup_fails {
                assert!(matches!(
                    report.failures[0].source,
                    aktor::AktorFailureSource::Actor {
                        phase: aktor::AktorPhase::End,
                        ..
                    }
                ));
            }
        }
    }
}

#[cfg(feature = "local")]
#[tokio::test]
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
                        stuck: aktor::AktorGroupStuck::KeepRunning,
                        actors: AktorNew {
                            name: "local closure".into(),
                            kind: AktorKind::TokioLocal(&executor),
                            start: aktor::AktorInit::Ready(async || ()),
                            end: async move |_| {
                                entered.notify_one();
                                release.notified().await;

                                if cleanup_fails {
                                    Err(((), String::from("real cleanup failure")))
                                } else {
                                    Ok(())
                                }
                            },
                            hooks: AktorHooks::NONE,
                            capacity: NonZeroUsize::new(32).unwrap(),
                            stuck: AktorStuck::Kill,
                        },
                        shutdown: |_| {},
                        grace: Duration::from_secs(5),
                    });
                    actors.ready().await.unwrap();

                    actors.killswitch().stop();
                    entering.notified().await;

                    let request = local::Request::new(
                        actors.handles(),
                        aktor::operation::Operation {
                            name: "rejected",
                            caller: std::panic::Location::caller(),
                        },
                        async |_: &mut (), ()| panic!("rejected call ran"),
                        (),
                    );

                    let error = match mode {
                        0 => request.await,
                        1 => request.send().await.map(drop),
                        2 => request.cast().await,
                        _ => request.timeout(Duration::from_secs(1)).await.unwrap(),
                    }
                    .unwrap_err();
                    assert_eq!(error.outcome, AktorDeliveryOutcome::NotAdmitted);
                    released.notify_one();

                    let report = actors.completion().await.unwrap();

                    assert_eq!(report.failed(), cleanup_fails);

                    if cleanup_fails {
                        assert!(matches!(
                            report.failures[0].source,
                            aktor::AktorFailureSource::Actor {
                                phase: aktor::AktorPhase::End,
                                ..
                            }
                        ));
                    }
                }
            }
        })
        .await;
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn retry() {
    let (actor, _group) = thread_actor(0, 1).await;
    let release = hold(&actor).await;
    call(&actor, async |s| *s += 1).cast().await.unwrap();

    let mut request = call(&actor, async |s| *s += 10);
    assert!(poll(&mut request).is_pending());
    release.send(()).unwrap();
    request.cast().await.unwrap();

    let release = hold(&actor).await;
    let mut submitted = call(&actor, async |s| *s += 100);
    assert!(poll(&mut submitted).is_pending());
    submitted.cast().await.unwrap();
    release.send(()).unwrap();

    assert_eq!(call(&actor, async |s| *s).await, Ok(111));
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn consumed() {
    let (actor, _group) = thread_actor((), 1).await;
    let mut request = call(&actor, async |_| ());
    (&mut request).await.unwrap();

    assert!(panics(request.cast()).await);
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn closed() {
    use message::AktorDeliveryOutcome;

    for release_first in [false, true] {
        let (actor, _group) = thread_actor((), 1).await;
        let release = hold(&actor).await;
        let queued = call(&actor, async |_| ()).send().await.unwrap();
        let mut waiting = call(&actor, async |_| ());
        assert!(poll(&mut waiting).is_pending());

        if release_first {
            release.send(()).unwrap();
            queued.await.unwrap();
            actor.shutdown().await.unwrap();
        } else {
            let done = actor.shutdown();
            release.send(()).unwrap();
            done.await.unwrap();
        }

        assert!(matches!(
            waiting.send().await,
            Err(aktor::AktorDeliveryError {
                outcome: AktorDeliveryOutcome::NotAdmitted,
                ..
            })
        ));
    }
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn reply() {
    let (actor, _group) = thread_actor(3, 1).await;
    let release = hold(&actor).await;
    let first: message::Reply<Result<usize, String>> =
        call(&actor, async |s| Ok(*s)).send().await.unwrap();

    let mut request = call(&actor, async |s| *s + 1);
    assert!(poll(&mut request).is_pending());
    release.send(()).unwrap();
    let reply: message::Reply<usize> = request.send().await.unwrap();

    assert_eq!(tokio::spawn(first).await.unwrap(), Ok(Ok(3)));
    assert_eq!(tokio::spawn(reply).await.unwrap(), Ok(4));
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn owner_failure() {
    let (release, closing) = tokio::sync::oneshot::channel();
    let group = aktor_start(AktorSetup {
        actors: support::thread_actor((), 1),
        shutdown: async move |_| {
            let _ = closing.await;
        },
        grace: Duration::from_secs(5),
        stuck: AktorGroupStuck::KeepRunning,
    });
    group.ready().await.unwrap();
    let actor = group.handles();
    assert_eq!(
        call(actor, async |_| Err::<(), _>("domain")).await,
        Ok(Err("domain"))
    );

    let error = tokio::time::timeout(
        Duration::from_secs(1),
        call(actor, async |_| panic!("operation failed")),
    )
    .await
    .expect("call waited for group shutdown")
    .unwrap_err();
    release.send(()).unwrap();
    let report = group.completion().await.unwrap();
    let failure = report.failure(error.failure_id.unwrap()).unwrap();
    assert!(failure.delivered());
    assert!(error.report.contains("operation failed"));
}

#[cfg(feature = "testing")]
#[tokio::test(flavor = "current_thread")]
async fn cooperative() {
    let (actor, _group) = thread_actor(0, 1024).await;
    let sent = std::cell::Cell::new(0);

    let producer = async {
        for _ in 0..1024 {
            call(&actor, async |state| *state += 1)
                .cast()
                .await
                .unwrap();
            sent.set(sent.get() + 1);
        }
    };
    let (_, observed) = tokio::join!(biased; producer, async { sent.get() });

    assert_eq!(call(&actor, async |s| *s).await, Ok(1024));
    assert!(observed < 1024, "producer monopolised the executor");
}
