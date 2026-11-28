use super::*;
use core::num::NonZeroUsize;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_start() {
    macro_rules! leave {
        ($kind:expr) => {{
            let (enter, entered) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel::<()>();
            let startup = aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "blocked start".into(),
                    kind: $kind,
                    start: aktor::AktorInit::Ready(async move || {
                        enter.send(()).unwrap();
                        let _ = blocked.recv();
                    }),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::MIN,
                    stuck: AktorStuck::Leave,
                },
                shutdown: |_| {},
                grace: Duration::from_millis(25),
            });
            let completion = startup.completion();

            entered.await.unwrap();
            startup.killswitch().stop();

            let stopped = tokio::time::timeout(Duration::from_secs(2), startup.ready()).await;
            drop(release);

            let (error, _) = stopped.unwrap().err().unwrap();
            assert_eq!(error.reason(), AktorStartReason::Stopping);
            assert_eq!(
                error.startup().unwrap().report.as_ref().unwrap().actors[0].retirement(),
                Some(AktorRetirement::StillRunning),
            );
            let _ = completion.wait_retired().await.unwrap();
        }};
    }

    leave!(AktorKind::TokioThread);
    leave!(AktorKind::TokioTask);
}

#[cfg(feature = "local")]
#[cfg(feature = "std_thread")]
#[tokio::test]
async fn stopping() {
    async fn stop<Handles, Group: setup::group::AktorSetupGroup, E>(
        started: AktorStarted<Handles, Group, E>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    ) {
        let completion = started.completion();

        entered.notified().await;
        Group::stop(&started.killswitch());
        release.notify_one();
        assert!(
            started.ready().await.is_err(),
            "stopping startup returned a handle"
        );

        let report = completion.await.unwrap();

        assert!(!report.ran_past_grace(), "{report}");
        assert_eq!(report.actors.len(), 1);
    }

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            for driven in [false, true] {
                let entered = Arc::new(Notify::new());
                let release = Arc::new(Notify::new());
                let begin = entered.clone();
                let gate = release.clone();
                let cleaned = Rc::new(Cell::new(0));
                let end = cleaned.clone();
                let setup = async move || {
                    begin.notify_one();
                    gate.notified().await;
                    Ok::<_, (core::convert::Infallible, String)>(0_u32)
                };

                let cleanup = async move |_: u32| {
                    end.set(end.get() + 1);
                };

                if driven {
                    stop(
                        aktor_start(AktorSetup {
                            stuck: aktor::AktorGroupStuck::KeepRunning,
                            actors: AktorNew {
                                name: "driven startup".into(),
                                kind: AktorKind::Local::<local::clock::Tokio, _, _>(|future| {
                                    tokio::task::spawn_local(future);
                                    Ok::<_, ((), String)>(())
                                }),
                                start: aktor::AktorInit::Try(setup),
                                end: cleanup,
                                hooks: AktorHooks::NONE,
                                capacity: NonZeroUsize::new(32).unwrap(),
                                stuck: AktorStuck::Kill,
                            },
                            shutdown: |_| {},
                            grace: Duration::from_secs(5),
                        }),
                        entered,
                        release,
                    )
                    .await;
                } else {
                    stop(
                        aktor_start(AktorSetup {
                            stuck: aktor::AktorGroupStuck::KeepRunning,
                            actors: AktorNew {
                                name: "spawned startup".into(),
                                kind: AktorKind::TokioLocal(&executor),
                                start: aktor::AktorInit::Try(setup),
                                end: cleanup,
                                hooks: AktorHooks::NONE,
                                capacity: NonZeroUsize::new(32).unwrap(),
                                stuck: AktorStuck::Kill,
                            },
                            shutdown: |_| {},
                            grace: Duration::from_secs(5),
                        }),
                        entered,
                        release,
                    )
                    .await;
                }

                assert_eq!(cleaned.get(), 1);
            }
        })
        .await;
}

#[tokio::test]
async fn existing() {
    let group = AktorGroup::new();
    let ended = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = ended.clone();
    let setup = AktorNew {
        name: "prepared counter".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Ready(async || 17_u32),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    let attempt = group.spawn(setup);
    let (error, _) = attempt.ready().await.err().unwrap();

    assert!(matches!(error, aktor::AktorStartError::Refused { .. }));
    assert!(!group.killswitch().is_stopping());

    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "first".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || 17_u32),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: move |_| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        },
        grace: Duration::from_millis(200),
    });
    started.ready().await.unwrap();
    let completion = started.completion();
    let group = started.group();
    let second = group
        .spawn(AktorNew {
            name: "second".into(),
            kind: AktorKind::TokioTask,
            start: aktor::AktorInit::Ready(async || 23_u32),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        })
        .ready()
        .await
        .unwrap();

    assert!(!group.killswitch().is_stopping());
    assert_eq!(user_count(started.handles()).await, Ok(17));
    assert_eq!(user_count(&second).await, Ok(23));

    let report = group.stop().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(report.actors.len(), 2);
    assert_eq!(completion.await.unwrap().actors.len(), 2);
    assert_eq!(ended.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[cfg(feature = "local")]
#[tokio::test]
async fn existing_local() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let mut group = local::AktorGroup::<local::clock::Tokio>::new();
            let driver = executor.spawn_local(group.listen(async |_| ()).unwrap());
            let actors = group
                .spawn(AktorNew {
                    name: "existing local counter".into(),
                    kind: AktorKind::Local::<local::clock::Tokio, _, _>(|future| {
                        tokio::task::spawn_local(future);
                        Ok::<_, ((), String)>(())
                    }),
                    start: aktor::AktorInit::Ready(async || 17_u32),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                })
                .ready()
                .await
                .unwrap();

            assert!(!group.killswitch().is_stopping());
            assert_eq!(user_count(&actors).await, Ok(17));

            let report = group.stop().await;

            assert!(!report.failed(), "{report}");
            assert_eq!(driver.await.unwrap().actors.len(), 1);

            let mut group = local::AktorGroup::<local::clock::Tokio>::new();
            let driver = executor.spawn_local(group.listen(async |_| ()).unwrap());
            let cleaned = Rc::new(Cell::new(0));
            let counted = cleaned.clone();
            let attempt = group.spawn((
                AktorNew {
                    name: "local rollback".into(),
                    kind: AktorKind::Local::<local::clock::Tokio, _, _>(|future| {
                        tokio::task::spawn_local(future);
                        Ok::<_, ((), String)>(())
                    }),
                    start: aktor::AktorInit::Ready(async || ()),
                    end: async move |_: ()| {
                        counted.set(counted.get() + 1);
                        Err(((), String::from("local rollback refused")))
                    },
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
                AktorNew {
                    name: "local setup failure".into(),
                    kind: AktorKind::Local::<local::clock::Tokio, _, _>(|future| {
                        tokio::task::spawn_local(future);
                        Ok::<_, ((), String)>(())
                    }),
                    start: aktor::AktorInit::Try(async || {
                        Err::<(), _>(((), String::from("local setup refused")))
                    }),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
            ));
            let (error, error_report) = attempt.ready().await.err().unwrap();

            assert!(error_report.contains("local setup refused"));
            assert!(matches!(
                error,
                aktor::AktorStartError::Initializer {
                    error: AktorBranch::Second(()),
                    ..
                }
            ));

            let report = error.startup().unwrap().report.as_ref().unwrap();

            assert!(matches!(
                report.stop_cause,
                aktor::StopCause::StartupFailure(_)
            ));
            assert!(!report.ran_past_grace(), "{report}");
            assert!(report.to_string().contains("local rollback refused"));
            assert_eq!(cleaned.get(), 1);
            assert!(
                driver
                    .await
                    .unwrap()
                    .to_string()
                    .contains("local rollback refused")
            );
        })
        .await;
}

#[tokio::test]
async fn origin() {
    #[aktor]
    async fn setup(_: &mut u32) {
        panic!("operation failed");
    }

    for mode in 0..3 {
        let runtime_failure = mode == 2;
        let (send, received) = tokio::sync::oneshot::channel();
        let actors = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "running counter".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: move |report| {
                let _sent = send.send(report);
            },
            grace: Duration::from_secs(5),
        });
        actors.ready().await.unwrap();

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let signal = entered.clone();
        let gate = release.clone();
        let registration = actors.group().new_registration();
        let opening = tokio::spawn(async move {
            let attempt = registration.spawn(AktorNew {
                name: "bad settings".into(),
                kind: AktorKind::TokioTask,
                start: aktor::AktorInit::Try(async move || {
                    signal.notify_one();
                    gate.notified().await;

                    if mode == 1 {
                        panic!("initialization failed");
                    }

                    Err::<u32, _>(((), String::from("invalid settings")))
                }),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            });
            let (error, _) = attempt.ready().await.err().unwrap();
            (
                error.reason(),
                error.startup().unwrap().report.clone().unwrap(),
            )
        });

        entered.notified().await;

        if runtime_failure {
            setup(actors.handles()).cast().await.unwrap();
            actors.killswitch().wait_stopping().await;
        }

        release.notify_one();
        let (reason, report) = tokio::time::timeout(Duration::from_secs(2), opening)
            .await
            .unwrap()
            .unwrap();
        let notification = received.await.unwrap();

        if mode == 1 {
            assert_eq!(reason, AktorStartReason::Init);
        }

        assert_eq!(
            matches!(notification.stop_cause, aktor::StopCause::StartupFailure(_)),
            !runtime_failure
        );
        assert_eq!(
            matches!(report.stop_cause, aktor::StopCause::StartupFailure(_)),
            matches!(notification.stop_cause, aktor::StopCause::StartupFailure(_))
        );
        assert!(notification.failed());
    }
}

#[tokio::test]
async fn overlapping() {
    let entered_a = Arc::new(Notify::new());
    let entered_b = Arc::new(Notify::new());
    let release_a = Arc::new(Notify::new());
    let release_b = Arc::new(Notify::new());
    let beginning_a = entered_a.clone();
    let beginning_b = entered_b.clone();
    let gate_a = release_a.clone();
    let gate_b = release_b.clone();
    let first = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "database".into(),
            kind: AktorKind::TokioTask,
            start: aktor::AktorInit::Try(async move || {
                beginning_a.notify_one();
                gate_a.notified().await;
                Err::<(), _>(((), String::from("first initialization refused")))
            }),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    let group = first.group();
    let second = group.spawn(AktorNew {
        name: "database".into(),
        kind: AktorKind::TokioTask,
        start: aktor::AktorInit::Ready(async move || {
            beginning_b.notify_one();
            gate_b.notified().await;
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    });
    let kill = group.killswitch();
    let release = async {
        entered_a.notified().await;
        entered_b.notified().await;
        release_a.notify_one();
        kill.wait_stopping().await;
        release_b.notify_one();
    };
    let (first, second, _) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(first.ready(), second.ready(), release)
    })
    .await
    .expect("overlapping startup did not settle");
    let (first, first_text) = first.err().unwrap();
    let (second, second_text) = second.err().unwrap();

    assert_eq!(
        first_text.matches("first initialization refused").count(),
        1,
        "{first_text}"
    );
    assert!(
        second_text.contains("first initialization refused"),
        "{second_text}"
    );
    assert_eq!(second.reason(), AktorStartReason::Stopping);
    assert_eq!(first.reason(), AktorStartReason::Init);
    assert!(matches!(
        second
            .startup()
            .unwrap()
            .report
            .as_ref()
            .unwrap()
            .stop_cause,
        aktor::StopCause::StartupFailure(_)
    ));
    assert!(matches!(
        first.startup().unwrap().report.as_ref().unwrap().stop_cause,
        aktor::StopCause::StartupFailure(_)
    ));
}
