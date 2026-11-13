use super::*;
use aktor::listener::{FailurePolicy, SpawnArgs};
use std::time::Duration;

#[tokio::test]
async fn cancel() {
    macro_rules! cancelling {
        ($kind:expr, $retirement:expr) => {{
            let entered = Arc::new(Notify::new());
            let beginning = entered.clone();
            let cleaned = Arc::new(Notify::new());
            let end = cleaned.clone();

            let startup = aktor_start(AktorSetup {
                actors: (
                    AktorNew {
                        name: "ready counter".into(),
                        kind: AktorKind::TokioThread,
                        start: async || aktor_ready(0_u32),
                        end: async move |_: u32| {
                            end.notify_one();
                        },
                        hooks: Default::default(),
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                    AktorNew {
                        name: "waiting counter".into(),
                        kind: $kind,
                        start: async move || {
                            beginning.notify_one();
                            std::future::pending::<Result<u32, String>>().await
                        },
                        end: (),
                        hooks: Default::default(),
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                ),
                shutdown: |_| {},
                grace: Duration::from_secs(1),
            });

            let completion = startup.completion();
            let running = tokio::spawn(startup);

            entered.notified().await;
            running.abort();
            assert!(matches!(running.await, Err(error) if error.is_cancelled()));
            tokio::time::timeout(Duration::from_secs(2), cleaned.notified())
                .await
                .unwrap();

            let report = tokio::time::timeout(Duration::from_secs(2), completion)
                .await
                .unwrap();

            assert!(report.startup);
            assert!(report.ran_past_grace);
            assert_eq!(
                report
                    .actors
                    .iter()
                    .find(|actor| actor.actor == "waiting counter")
                    .unwrap()
                    .retirement,
                $retirement
            );
        }};
    }

    cancelling!(AktorKind::TokioThread, AktorRetireStatus::Terminated);
    cancelling!(AktorKind::TokioTask, AktorRetireStatus::Terminated);

    let startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "never started".into(),
            kind: AktorKind::TokioThread,
            start: async || -> Result<u32, String> { panic!("stopped startup ran") },
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });

    let kill = startup.killswitch();
    let completion = startup.completion();

    kill.stop();
    let (failure, _) = startup.await.err().unwrap();
    assert_eq!(failure.init, AktorInitOutcome::NotRun);
    assert!(completion.await.actors.is_empty());

    let startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "discarded startup".into(),
            kind: AktorKind::TokioThread,
            start: async || -> Result<u32, String> { panic!("discarded startup ran") },
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });

    let kill = startup.killswitch();
    let completion = startup.completion();

    drop(startup);

    let report = tokio::time::timeout(Duration::from_millis(200), completion)
        .await
        .expect("discarded startup left its completion pending");

    assert!(report.actors.is_empty());
    assert!(kill.is_stopping());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_start() {
    macro_rules! leave {
        ($kind:expr) => {{
            let (enter, entered) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel::<()>();
            let startup = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: "blocked start".into(),
                    kind: $kind,
                    start: async move || {
                        enter.send(()).unwrap();
                        let _ = blocked.recv();
                        aktor_ready(())
                    },
                    end: (),
                    hooks: Default::default(),
                    capacity: 1,
                    stuck: AktorStuck::Leave,
                },
                shutdown: |_| {},
                grace: Duration::from_millis(25),
            });
            let kill = startup.killswitch();
            let completion = startup.completion();
            let mut startup = Box::pin(startup);

            tokio::select! {
                _ = &mut startup => panic!("startup completed while blocked"),
                result = entered => result.unwrap(),
            }

            kill.stop();
            let stopped = tokio::time::timeout(Duration::from_secs(2), startup).await;
            drop(release);

            let (error, _) = stopped.unwrap().err().unwrap();
            assert_eq!(error.init, AktorInitOutcome::Unconfirmed);
            assert_eq!(
                error.report.unwrap().actors[0].retirement,
                AktorRetireStatus::Unconfirmed,
            );
            completion.wait_retired().await.unwrap();
        }};
    }

    leave!(AktorKind::TokioThread);
    leave!(AktorKind::TokioTask);
}

#[cfg(feature = "local")]
#[cfg(feature = "std_thread")]
#[tokio::test]
async fn stopping() {
    async fn stop<Setup: setup::AktorStart>(
        startup: setup::AktorStartup<Setup>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    ) where
        <Setup::Group as setup::group::AktorSetupGroup>::Completion:
            core::future::IntoFuture<Output = AktorShutdownReport>,
    {
        let kill = startup.killswitch();
        let completion = startup.completion();

        tokio::pin!(startup);
        tokio::select! {
            _ = &mut startup => panic!("setup finished before its gate"),
            _ = entered.notified() => {},
        }
        <Setup::Group as setup::group::AktorSetupGroup>::stop(&kill);
        release.notify_one();
        assert!(startup.await.is_err(), "stopping startup returned a handle");

        let report = completion.await;

        assert!(!report.ran_past_grace, "{report}");
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
                    Ok::<_, String>(0_u32)
                };

                let cleanup = async move |_: u32| {
                    end.set(end.get() + 1);
                    Ok::<_, String>(())
                };

                if driven {
                    stop(
                        aktor_start(AktorSetup {
                            actors: AktorNew {
                                name: "driven startup".into(),
                                kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                                    tokio::task::spawn_local(future);
                                    Ok::<_, String>(())
                                }),
                                start: setup,
                                end: cleanup,
                                hooks: Default::default(),
                                capacity: 32,
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
                            actors: AktorNew {
                                name: "spawned startup".into(),
                                kind: AktorKind::TokioLocal(&executor),
                                start: setup,
                                end: cleanup,
                                hooks: Default::default(),
                                capacity: 32,
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
    let mut group = AktorGroup::with_grace(Duration::from_millis(200));
    let ended = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = ended.clone();
    let setup = AktorNew {
        name: "prepared counter".into(),
        kind: AktorKind::TokioThread,
        start: async || aktor_ready(17_u32),
        end: (),
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    };

    let (error, _) = aktor_start_in(&group, setup).await.err().unwrap();

    assert!(error.report.is_none());
    assert!(!group.killswitch().is_stopping());

    let completion = group
        .start_with(async move |_| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, String>(())
        })
        .unwrap();

    let handles = aktor_start_in(
        &group,
        (
            AktorNew {
                name: "first".into(),
                kind: AktorKind::TokioThread,
                start: async || aktor_ready(17_u32),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "second".into(),
                kind: AktorKind::TokioTask,
                start: async || aktor_ready(23_u32),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
        ),
    )
    .await
    .unwrap();

    assert!(!group.killswitch().is_stopping());
    assert_eq!(user_count(&handles.0).await, 17);
    assert_eq!(user_count(&handles.1).await, 23);

    let report = group.shutdown().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(report.actors.len(), 2);
    assert_eq!(completion.await.actors.len(), 2);
    assert_eq!(ended.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn existing_cancel() {
    let mut group = AktorGroup::with_grace(Duration::from_secs(1));
    let completion = group.start().unwrap();
    let previous_cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let previous = previous_cleaned.clone();
    let _healthy = aktor_start_in(
        &group,
        AktorNew {
            name: "existing healthy actor".into(),
            kind: AktorKind::TokioTask,
            start: async || aktor_ready(()),
            end: async move |_: ()| {
                previous.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, String>(())
            },
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
    )
    .await
    .unwrap();

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let signal = entered.clone();
    let gate = release.clone();
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = cleaned.clone();
    let setup = AktorNew {
        name: "cancelled prepared counter".into(),
        kind: AktorKind::TokioThread,
        start: async move || {
            signal.notify_one();
            gate.notified().await;
            Ok::<_, String>(17_u32)
        },
        end: async move |_: u32| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        },
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    };

    let mut startup = Box::pin(aktor_start_in(&group, setup));

    tokio::select! {
        _ = &mut startup => panic!("setup passed its gate"),
        _ = entered.notified() => {},
    }
    drop(startup);
    assert!(group.killswitch().is_stopping());
    release.notify_one();

    let report = completion.await;

    assert!(report.startup);
    assert!(!report.ran_past_grace, "{report}");
    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        previous_cleaned.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(group.completion().await.actors.len(), 2);
}

#[cfg(feature = "local")]
#[tokio::test]
async fn existing_local() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let mut group = local::AktorGroup::<local::clock::Tokio>::new();
            let driver = executor.spawn_local(group.listen().unwrap());
            let actors = aktor_start_in(
                &group,
                AktorNew {
                    name: "existing local counter".into(),
                    kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                        tokio::task::spawn_local(future);
                        Ok::<_, String>(())
                    }),
                    start: async || aktor_ready(17_u32),
                    end: (),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
            )
            .await
            .unwrap();

            assert!(!group.killswitch().is_stopping());
            assert_eq!(user_count(&actors).await, 17);

            let report = group.shutdown().await;

            assert!(!report.failed(), "{report}");
            assert_eq!(driver.await.unwrap().actors.len(), 1);

            let mut group = local::AktorGroup::<local::clock::Tokio>::new();
            let driver = executor.spawn_local(group.listen().unwrap());
            let cleaned = Rc::new(Cell::new(0));
            let counted = cleaned.clone();
            let (error, error_report) = aktor_start_in(
                &group,
                (
                    AktorNew {
                        name: "local rollback".into(),
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok::<_, String>(())
                        }),
                        start: async || aktor_ready(()),
                        end: async move |_: ()| {
                            counted.set(counted.get() + 1);
                            Err(String::from("local rollback refused"))
                        },
                        hooks: Default::default(),
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                    AktorNew {
                        name: "local setup failure".into(),
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok::<_, String>(())
                        }),
                        start: async || Err::<(), _>(String::from("local setup refused")),
                        end: (),
                        hooks: Default::default(),
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                ),
            )
            .await
            .err()
            .unwrap();

            assert!(error_report.contains("local setup refused"));
            let AktorStartCause::Init(original) = &error.cause else {
                panic!("local startup lost its typed error");
            };

            assert_eq!(original.diagnostics, "local setup refused");

            let report = error.report.unwrap();

            assert!(report.startup);
            assert!(!report.ran_past_grace, "{report}");
            assert!(report.to_string().contains("local rollback refused"));
            assert_eq!(cleaned.get(), 1);
            assert_eq!(driver.await.unwrap().to_string(), report.to_string());
        })
        .await;
}

#[tokio::test]
async fn standalone_cancel() {
    struct Probe(std::sync::mpsc::Sender<&'static str>, &'static str);
    impl Drop for Probe {
        fn drop(&mut self) {
            let _sent = self.0.send(self.1);
        }
    }
    thread_local! {
        static EXIT: std::cell::RefCell<Option<Probe>> = const { std::cell::RefCell::new(None) };
    }

    for standard in [false, true] {
        for _ in 0..2 {
            let entered = Arc::new(Notify::new());
            let signal = entered.clone();
            let (discarded, observed) = std::sync::mpsc::channel();
            let mut opening = Box::pin(listener::lifecycle::spawn::spawn_async_on(
                SpawnArgs {
                    name: "cancelled startup".into(),
                    capacity: 1,
                    failure: FailurePolicy::Unwind,
                    setup: move || async move {
                        let _probe = Probe(discarded.clone(), "setup");

                        EXIT.with(|exit| *exit.borrow_mut() = Some(Probe(discarded, "thread")));
                        signal.notify_one();
                        core::future::pending::<Result<(), AktorError>>().await
                    },
                    cleanup: |_| async { Ok::<_, String>(()) },
                },
                listener::hooks::CallHooks::default(),
                standard,
            ));

            tokio::select! {
                _ = &mut opening => panic!("pending setup finished"),
                _ = entered.notified() => {},
            }
            drop(opening);
            assert_eq!(
                observed.recv_timeout(Duration::from_secs(1)).unwrap(),
                "setup"
            );
            assert_eq!(
                observed.recv_timeout(Duration::from_secs(1)).unwrap(),
                "thread"
            );
        }
    }
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
            actors: AktorNew {
                name: "running counter".into(),
                kind: AktorKind::TokioThread,
                start: async || aktor_ready(0_u32),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
            shutdown: move |report| {
                let _sent = send.send(report);
            },
            grace: Duration::from_secs(5),
        })
        .await
        .unwrap();

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let signal = entered.clone();
        let gate = release.clone();
        let registration = actors.group.new_registration();
        let opening = tokio::spawn(async move {
            aktor_start_in(
                &registration,
                AktorNew {
                    name: "bad settings".into(),
                    kind: AktorKind::TokioTask,
                    start: async move || {
                        signal.notify_one();
                        gate.notified().await;

                        if mode == 1 {
                            panic!("initialization failed");
                        }

                        Err::<u32, _>(String::from("invalid settings"))
                    },
                    end: (),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
            )
            .await
        });

        entered.notified().await;

        if runtime_failure {
            setup(&actors.handles).cast().await;
            actors.killswitch().wait_stopping().await;
        }

        release.notify_one();
        let (error, _) = tokio::time::timeout(Duration::from_secs(2), opening)
            .await
            .unwrap()
            .unwrap()
            .err()
            .unwrap();
        let notification = received.await.unwrap();

        if mode == 1 {
            assert_eq!(error.init, AktorInitOutcome::Unconfirmed);
        }

        assert_eq!(notification.startup, !runtime_failure);
        assert_eq!(error.report.unwrap().startup, notification.startup);
        assert!(notification.failed());
    }
}

#[tokio::test]
async fn overlapping() {
    let mut group = AktorGroup::with_grace(Duration::from_secs(5));

    group.start().unwrap();

    let entered_a = Arc::new(Notify::new());
    let entered_b = Arc::new(Notify::new());
    let release_a = Arc::new(Notify::new());
    let release_b = Arc::new(Notify::new());
    let beginning_a = entered_a.clone();
    let beginning_b = entered_b.clone();
    let gate_a = release_a.clone();
    let gate_b = release_b.clone();
    let first = aktor_start_in(
        &group,
        AktorNew {
            name: "database".into(),
            kind: AktorKind::TokioTask,
            start: async move || {
                beginning_a.notify_one();
                gate_a.notified().await;
                Err::<(), _>(String::from("first initialization refused"))
            },
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
    );
    let second = aktor_start_in(
        &group,
        AktorNew {
            name: "database".into(),
            kind: AktorKind::TokioTask,
            start: async move || {
                beginning_b.notify_one();
                gate_b.notified().await;
                Ok::<_, String>(())
            },
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
    );
    let release = async {
        entered_a.notified().await;
        entered_b.notified().await;
        release_a.notify_one();
        group.killswitch().wait_stopping().await;
        release_b.notify_one();
    };
    let (first, second, _) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(first, second, release)
    })
    .await
    .expect("overlapping startup did not settle");
    let (first, first_text) = first.err().unwrap();
    let (second, second_text) = second.err().unwrap();

    assert_eq!(
        first_text.matches("first initialization refused").count(),
        1
    );
    assert!(
        second_text.contains("first initialization refused"),
        "{second_text}"
    );
    assert!(matches!(
        second.cause,
        AktorStartCause::Group(AktorGroupError::Stopping)
    ));
    assert_eq!(
        first.report.as_ref().unwrap().to_string(),
        second.report.as_ref().unwrap().to_string()
    );
    assert!(first.report.as_ref().unwrap().startup);
}
