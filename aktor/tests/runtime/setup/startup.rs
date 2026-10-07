use super::*;

#[tokio::test]
async fn cancel() {
    let entered = Arc::new(Notify::new());
    let beginning = entered.clone();
    let cleaned = Arc::new(Notify::new());
    let end = cleaned.clone();

    let startup = aktor_start(AktorSetup {
        actors: aktor_setups! {
                    ready: AktorNew {

                        name: AktorName::new("ready counter"),
                        role: AktorNoRole,
                        kind: AktorKind::TokioThread,
                        closures: AktorClosures {
        start: async || Ok::<_, AktorSetupError>(0_u32),

                            end: Some(
                                (async move |_: u32| {
                                    end.notify_one();
                                    Ok(())
                                })
                                .into(),
                            ),
                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: AktorNewOptions { capacity: 32 },
                    },
                    waiting: AktorNew {

                        name: AktorName::new("waiting counter"),
                        role: AktorNoRole,
                        kind: AktorKind::TokioThread,
                        closures: AktorClosures {
        start: async move || {
                            beginning.notify_one();
                            std::future::pending::<Result<u32, AktorSetupError>>().await
                        },

                            end: None,
                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: AktorNewOptions { capacity: 32 },
                    },
                },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(1),
        },
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
    assert!(report.timed_out);
    assert_eq!(report.actors.len(), 2);

    let startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("never started"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || -> Result<u32, AktorSetupError> { panic!("stopped startup ran") },
                end: None::<AktorClosure<dyn setup::AktorEnd<u32> + Send>>,
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
    });

    let kill = startup.killswitch();
    let completion = startup.completion();

    kill.stop();
    assert!(startup.await.is_err());
    assert!(completion.await.actors.is_empty());

    let startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("discarded startup"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || -> Result<u32, AktorSetupError> { panic!("discarded startup ran") },
                end: None::<AktorClosure<dyn setup::AktorEnd<u32> + Send>>,
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
            core::future::IntoFuture<Output = ShutdownReport>,
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

        assert!(!report.timed_out, "{report}");
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
                    Ok(0_u32)
                };

                let cleanup = async move |_: u32| {
                    end.set(end.get() + 1);
                    Ok(())
                };

                if driven {
                    stop(
                        aktor_start(AktorSetup {
                            actors: AktorNew {
                                name: AktorName::new("driven startup"),
                                role: AktorNoRole,
                                kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                                    tokio::task::spawn_local(future);
                                    Ok(())
                                }),
                                closures: AktorClosures {
                                    start: setup,
                                    end: Some(cleanup.into()),

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
                        }),
                        entered,
                        release,
                    )
                    .await;
                } else {
                    stop(
                        aktor_start(AktorSetup {
                            actors: AktorNew {
                                name: AktorName::new("spawned startup"),
                                role: AktorNoRole,
                                kind: AktorKind::TokioLocal(&executor),
                                closures: AktorClosures {
                                    start: setup,
                                    end: Some(cleanup.into()),

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
        name: AktorName::new("prepared counter"),
        role: aktors::Users,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok::<_, AktorSetupError>(17_u32),
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: AktorNewOptions { capacity: 32 },
    };

    let error = aktor_start_in(&group, setup).await.err().unwrap();

    assert!(error.report.is_none());
    assert!(!group.killswitch().is_stopping());

    let completion = group
        .start_with(async move |_| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, AktorCleanupError>(())
        })
        .unwrap();

    let handles = aktor_start_in(
        &group,
        aktor_setups! {
            first: AktorNew {

                name: AktorName::new("first"),
                role: aktors::Users,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
start: async || Ok::<_, AktorSetupError>(17_u32),  end: None, intervals: vec![], before_each: None, after_each: None },
                options: AktorNewOptions { capacity: 32 },
            },
            second: AktorNew {

                name: AktorName::new("second"),
                role: aktors::Users,
                kind: AktorKind::TokioTask,
                closures: AktorClosures {
start: async || Ok(23_u32),  end: None, intervals: vec![], before_each: None, after_each: None },
                options: AktorNewOptions { capacity: 32 },
            },
        },
    )
    .await
    .unwrap();

    assert!(!group.killswitch().is_stopping());
    assert_eq!(user_count(&handles.first).await, 17);
    assert_eq!(user_count(&handles.second).await, 23);

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
            name: AktorName::new("existing healthy actor"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(()),
                end: Some(
                    (async move |_: ()| {
                        previous.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(())
                    })
                    .into(),
                ),

                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
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
        name: AktorName::new("cancelled prepared counter"),
        role: aktors::Users,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async move || {
                signal.notify_one();
                gate.notified().await;
                Ok::<_, AktorSetupError>(17_u32)
            },
            end: Some(
                (async move |_: u32| {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
                .into(),
            ),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: AktorNewOptions { capacity: 32 },
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
    assert!(!report.timed_out, "{report}");
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
                    name: AktorName::new("existing local counter"),
                    role: aktors::Users,
                    kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                        tokio::task::spawn_local(future);
                        Ok(())
                    }),
                    closures: AktorClosures {
                        start: async || Ok(17_u32),
                        end: None,
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: AktorNewOptions { capacity: 32 },
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
            let error = aktor_start_in(
                &group,
                (
                    AktorNew {
                        name: AktorName::new("local rollback"),
                        role: AktorNoRole,
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok(())
                        }),
                        closures: AktorClosures {
                            start: async || Ok(()),
                            end: Some(
                                (async move |_: ()| {
                                    counted.set(counted.get() + 1);
                                    Err(AktorCleanupError::new("local rollback refused"))
                                })
                                .into(),
                            ),

                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: AktorNewOptions { capacity: 32 },
                    },
                    AktorNew {
                        name: AktorName::new("local setup failure"),
                        role: AktorNoRole,
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok(())
                        }),
                        closures: AktorClosures {
                            start: async || {
                                Err::<(), _>(AktorSetupError::new("local setup refused"))
                            },
                            end: None,
                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: AktorNewOptions { capacity: 32 },
                    },
                ),
            )
            .await
            .err()
            .unwrap();

            assert!(format!("{error:#}").contains("local setup refused"));
            let mut source = std::error::Error::source(&error);
            let mut owner = None;

            while let Some(error) = source {
                if let Some(error) = error.downcast_ref::<local::OwnerError>() {
                    owner = Some(error);
                    break;
                }

                source = error.source();
            }

            assert!(matches!(owner, Some(local::OwnerError::Setup(_))));

            let report = error.report.unwrap();

            assert!(report.startup);
            assert!(!report.timed_out, "{report}");
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
                        core::future::pending::<Result<(), AktorSetupError>>().await
                    },
                    cleanup: |_| async { Ok::<_, AktorCleanupError>(()) },
                },
                listener::hooks::AktorHooks::default(),
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
                name: AktorName::new("running counter"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(0_u32),
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
            },
            shutdown: move |report| {
                let _sent = send.send(report);
            },
            options: Default::default(),
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
                    name: AktorName::new("bad settings"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioTask,
                    closures: AktorClosures {
                        start: async move || {
                            signal.notify_one();
                            gate.notified().await;

                            if mode == 1 {
                                panic!("initialization failed");
                            }

                            Err::<u32, _>(AktorSetupError::new("invalid settings"))
                        },
                        end: None,
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: Default::default(),
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
        let error = tokio::time::timeout(Duration::from_secs(2), opening)
            .await
            .unwrap()
            .unwrap()
            .err()
            .unwrap();
        let notification = received.await.unwrap();

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
            name: AktorName::new("database"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async move || {
                    beginning_a.notify_one();
                    gate_a.notified().await;
                    Err::<(), _>(AktorSetupError::new("first initialization refused"))
                },
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
        },
    );
    let second = aktor_start_in(
        &group,
        AktorNew {
            name: AktorName::new("database"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async move || {
                    beginning_b.notify_one();
                    gate_b.notified().await;
                    Ok(())
                },
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
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
    let first = first.err().unwrap();
    let second = second.err().unwrap();
    let first_text = format!("{first:#}");
    let second_text = format!("{second:#}");

    assert_eq!(
        first_text.matches("first initialization refused").count(),
        1
    );
    assert!(
        second_text.contains("first initialization refused"),
        "{second_text}"
    );
    assert!(
        second_text.contains("actor task closed during startup"),
        "{second_text}"
    );
    assert_eq!(
        first.report.as_ref().unwrap().to_string(),
        second.report.as_ref().unwrap().to_string()
    );
    assert!(first.report.as_ref().unwrap().startup);
}
