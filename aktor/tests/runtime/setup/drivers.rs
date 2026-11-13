use super::*;
use std::time::Duration;

#[tokio::test]
async fn interval_launch() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            for accepted in [0, 1] {
                let launches = Rc::new(Cell::new(0));
                let launched = launches.clone();
                let cleaned = Rc::new(Cell::new(0));
                let end = cleaned.clone();
                let observed = Rc::new(std::cell::RefCell::new(None));
                let callback = observed.clone();
                let tasks = Rc::new(std::cell::RefCell::new(Vec::new()));
                let spawned = tasks.clone();
                let startup = aktor_start(AktorSetup {
                    actors: AktorNew {
                        name: "local interval launch".into(),
                        kind: AktorKind::Local::<local::clock::Tokio>(move |future| {
                            let index = launched.get();
                            launched.set(index + 1);

                            if index == accepted + 1 {
                                return Err(String::from("interval launcher rejected"));
                            }

                            spawned.borrow_mut().push(tokio::task::spawn_local(future));
                            Ok(())
                        }),
                        start: async || aktor_ready(()),
                        end: async move |_: ()| end.set(end.get() + 1),
                        hooks: AktorHooks {
                            intervals: (0..=accepted)
                                .map(|_| AktorInterval {
                                    every: Duration::from_secs(60),
                                    run: (async |_: &mut ()| {}).into(),
                                })
                                .collect(),
                            before_each: None,
                            after_each: None,
                        },
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: move |report| *callback.borrow_mut() = Some(report),
                    grace: Duration::from_secs(1),
                });
                let completion = startup.completion();
                let report_text = match startup.await {
                    Ok(_) => panic!("rejected interval launch returned an actor"),
                    Err((_, report)) => report,
                };
                let report = completion.await;
                let tasks = std::mem::take(&mut *tasks.borrow_mut());

                for task in tasks {
                    tokio::time::timeout(Duration::from_secs(1), task)
                        .await
                        .expect("accepted launcher task survived rollback")
                        .unwrap();
                }

                assert!(report_text.contains("interval launcher rejected"));
                assert_eq!(cleaned.get(), 1);

                for report in [&report, observed.borrow().as_ref().unwrap()] {
                    assert!(report.failed(), "{report}");
                    assert!(!report.ran_past_grace, "{report}");

                    assert_eq!(
                        report
                            .to_string()
                            .matches("interval launcher rejected")
                            .count(),
                        1
                    );
                }
            }
        })
        .await;
}

#[tokio::test]
async fn custom() {
    struct RunnerDrop(bool);
    impl Drop for RunnerDrop {
        fn drop(&mut self) {
            if self.0 {
                panic!("pending runner drop died");
            }
        }
    }

    let executor = Rc::new(tokio::task::LocalSet::new());

    executor
        .run_until(async {
            for mode in 0..4 {
                let spawn = executor.clone();
                let entered = Arc::new(Notify::new());
                let signal = entered.clone();
                let held = RunnerDrop(mode == 3);
                let ended = Rc::new(Cell::new(None));
                let end = ended.clone();
                let cleanup_calls = Rc::new(Cell::new(0));
                let count = cleanup_calls.clone();
                let before = Rc::new(Cell::new(0));
                let log = before.clone();
                let actors = aktor_start(AktorSetup {
                    actors: AktorNew {
                        name: "custom counter".into(),
                        kind: AktorKind::Custom(
                            local::clock::Tokio,
                            move |future| {
                                spawn.spawn_local(future);
                                Ok(())
                            },
                            async move |mut runner: AktorRunner<'_, Counter, 0, ()>| {
                                let _keep = &held;

                                if mode == 2 {
                                    signal.notified().await;
                                    return Ok(());
                                }

                                if mode == 3 {
                                    signal.notify_one();
                                    core::future::pending::<()>().await;
                                }

                                while let Some(call) = runner.next().await {
                                    if mode == 1 {
                                        drop(call);
                                    } else {
                                        call.run().await;
                                    }
                                }

                                Ok(())
                            },
                        ),
                        start: async || aktor_ready(Counter(Rc::new(Cell::new(0)))),
                        end: async move |state: Counter| {
                            count.set(count.get() + 1);
                            end.set(Some(state.0.get()));
                            Ok::<_, String>(())
                        },
                        hooks: AktorHooks {
                            intervals: vec![],
                            before_each: Some(
                                (move |_: &mut Counter, _: operation::Operation| {
                                    log.set(log.get() + 1);
                                })
                                .into(),
                            ),
                            after_each: None,
                        },
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_millis(100),
                })
                .await
                .unwrap();

                if mode == 3 {
                    entered.notified().await;

                    let report = actors.shutdown().await;

                    assert!(report.ran_past_grace, "{report}");
                    // The hanging runner gets cut after its grace, `end` still runs.
                    assert_eq!(ended.get(), Some(0));
                    let text = report.to_string();
                    assert_eq!(
                        text.matches("pending runner drop died").count(),
                        1,
                        "{text}"
                    );
                } else if mode == 2 {
                    entered.notify_one();

                    let report = actors.completion().await;

                    assert!(report.failed(), "{report}");
                    assert_eq!(ended.get(), Some(0));
                    assert!(
                        report
                            .failure
                            .unwrap()
                            .message
                            .contains("before draining admission")
                    );
                } else if mode == 1 {
                    drop(add(&actors.handles, 3).send().await);
                    tokio::time::timeout(
                        Duration::from_millis(200),
                        actors.killswitch().wait_stopping(),
                    )
                    .await
                    .expect("discarded call left the custom runner alive");

                    let report = actors.completion().await;

                    assert!(report.failed(), "{report}");
                    assert_eq!(ended.get(), Some(0));
                    assert!(
                        report
                            .failure
                            .unwrap()
                            .message
                            .contains("discarded admitted work")
                    );
                } else {
                    assert_eq!(add(&actors.handles, 0).await, Err("give me something"));
                    assert_eq!(add(&actors.handles, 3).await, Ok(3));

                    let (input, mut output) = add::latest(&actors.handles);

                    input.send(4);

                    let report = actors.shutdown().await;

                    assert!(!report.failed(), "{report}");
                    assert_eq!(ended.get(), Some(7));
                    assert_eq!(before.get(), 3);
                    assert_eq!(output.next().await, Some(Ok(7)));
                    assert!(output.next().await.is_none());
                    drop(input);
                }

                assert_eq!(cleanup_calls.get(), 1);
            }
        })
        .await;
}

#[tokio::test]
async fn local_panic() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    for phase in 0..4 {
        for cleanup_failure in 0..3 {
            let (handle, mut owner) = local::channel::<u32, 1, ()>().unwrap();

            if phase == 1 {
                owner.hooks.before_each = Some(Box::new(|_, _| panic!("before died")));
            }

            if phase == 2 {
                owner.hooks.after_each = Some(Box::new(|_, _| panic!("after died")));
            }

            let name = match phase {
                0 => "operation died",
                1 => "before died",
                2 => "after died",
                _ => "custom died",
            };

            local::Request::new(
                &handle,
                operation::Operation {
                    name: "panic",
                    caller: std::panic::Location::caller(),
                },
                async move |_: &mut u32, ()| {
                    if phase == 0 {
                        panic!("operation died");
                    }
                },
                (),
            )
            .cast()
            .await;

            let cleaned = Rc::new(Cell::new(0));
            let cleanup = cleaned.clone();
            let completion = owner.completion();
            let result = AssertUnwindSafe(owner.run_custom(
                7,
                async move |_| {
                    cleanup.set(cleanup.get() + 1);

                    match cleanup_failure {
                        1 => Err(AktorError::new("cleanup refused", ())),
                        2 => panic!("cleanup died"),
                        _ => Ok(()),
                    }
                },
                async move |runner: local::AktorRunner<'_, u32, 1>| {
                    if phase == 3 {
                        panic!("custom died");
                    }

                    local::serve(runner).await
                },
            ))
            .catch_unwind()
            .await
            .unwrap_err();

            assert_eq!(result.downcast_ref::<&str>(), Some(&name));
            assert_eq!(cleaned.get(), 1);

            let diagnostics = completion.diagnostics();

            assert_eq!(diagnostics.len(), usize::from(cleanup_failure != 0));
            if cleanup_failure == 1 {
                assert_eq!(
                    diagnostics[0].diagnostics,
                    "actor cleanup failed: cleanup refused"
                );
            }
            assert!(completion.wait().await.is_err());
        }
    }
}

#[cfg(feature = "embassy_cross_core")]
#[tokio::test]
async fn cross_core_panic() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    #[aktor]
    async fn crash(_: &mut u32) {
        panic!("cross core died");
    }

    for cleanup_failure in 0..3 {
        let (handle, owner) = cross_core::channel::<u32>(1).unwrap();
        let completion = handle.completion();

        crash(&handle).cast().await;

        let cleaned = Rc::new(Cell::new(0));
        let cleanup = cleaned.clone();
        let result = AssertUnwindSafe(owner.run_with(
            async || Ok(7),
            async move |_| {
                cleanup.set(cleanup.get() + 1);

                match cleanup_failure {
                    1 => Err(AktorError::new("cleanup refused", ())),
                    2 => panic!("cleanup died"),
                    _ => Ok(()),
                }
            },
        ))
        .catch_unwind()
        .await;

        assert_eq!(
            result.unwrap_err().downcast_ref::<&str>(),
            Some(&"cross core died")
        );
        assert_eq!(cleaned.get(), 1);
        assert!(completion.wait().await.is_err());
        assert_eq!(completion.diagnostics().is_empty(), cleanup_failure == 0);
    }
}

#[tokio::test]
async fn local() {
    let executor = tokio::task::LocalSet::new();
    let count = Rc::new(Cell::new(0));
    let before = count.clone();
    let finished = Rc::new(Cell::new(0));
    let end = finished.clone();
    let thread = |name: &str| AktorNew {
        name: name.into(),
        kind: AktorKind::TokioThread,
        start: async || aktor_ready(85_u32),
        end: (),
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    };

    let actors = executor
        .run_until(aktor_start(AktorSetup {
            actors: (
                thread("first"),
                AktorNew {
                    name: "local counter".into(),
                    kind: AktorKind::TokioLocal(&executor),
                    start: async || aktor_ready(Counter(Rc::new(Cell::new(0)))),
                    end: async move |counter: Counter| {
                        end.set(counter.0.get());
                        Ok::<_, String>(())
                    },
                    hooks: AktorHooks {
                        intervals: vec![],
                        before_each: Some(
                            (move |_: &mut Counter, _: operation::Operation| {
                                before.set(before.get() + 1);
                            })
                            .into(),
                        ),
                        after_each: None,
                    },
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                thread("last"),
            ),
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        }))
        .await
        .unwrap();

    assert_eq!(
        message::call(&actors.handles.0.handle, |state, ()| *state, ()).await,
        85
    );
    assert_eq!(
        message::call(&actors.handles.2.handle, |state, ()| *state, ()).await,
        85
    );

    tokio::time::pause();

    let mut request = Box::pin(local_add(&actors.handles.1, 10).timeout(Duration::from_secs(85)));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());

    assert!(request.as_mut().poll(&mut context).is_pending());
    tokio::time::advance(Duration::from_secs(86)).await;
    assert!(matches!(
        request.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(AktorTimeoutError { admitted: true, .. }))
    ));
    drop(request);
    tokio::time::resume();

    executor
        .run_until(async {
            assert_eq!(local_add(&actors.handles.1, 5).await, Ok(15));

            let (sender, mut results) = local_add::latest(&actors.handles.1);

            sender.send(3);
            assert_eq!(results.next().await, Some(Ok(18)));

            assert!(!actors.shutdown().await.failed());
            assert_eq!(count.get(), 3);
            assert_eq!(finished.get(), 18);

            let actors = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: "supplied executor".into(),
                    kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                        tokio::task::spawn_local(future);
                        Ok(())
                    }),
                    start: async || aktor_ready(Counter(Rc::new(Cell::new(0)))),
                    end: (),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
            .await
            .unwrap();

            assert_eq!(local_add(&actors.handles, 85).await, Ok(85));

            let (sender, mut results) = local_add::latest(&actors.handles);

            sender.send(1);
            assert_eq!(results.next().await, Some(Ok(86)));

            let report = actors.shutdown().await;

            assert!(!report.failed(), "{report}");
        })
        .await;
}

#[tokio::test(start_paused = true)]
async fn local_time() {
    use futures_util::FutureExt;
    use local::clock::{AktorGroupClock, Tokio};

    tokio::time::advance(Duration::from_secs(30)).await;

    let mut wait = Tokio::wait(Tokio::deadline(Duration::from_secs(5)));

    assert!((&mut wait).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!((&mut wait).now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(1)).await;
    wait.await;

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let ticks = Rc::new(Cell::new(0));
            let ticked = Rc::new(Notify::new());
            let count = ticks.clone();
            let notify = ticked.clone();
            let cleaned = Rc::new(Cell::new(0));
            let end = cleaned.clone();
            let actors = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: "virtual time".into(),
                    kind: AktorKind::Local::<Tokio>(|future| {
                        tokio::task::spawn_local(future);
                        Ok(())
                    }),
                    start: async || aktor_ready(()),
                    end: async move |_: ()| {
                        tokio::time::sleep(Duration::from_secs(4)).await;
                        end.set(end.get() + 1);
                        Ok::<_, String>(())
                    },
                    hooks: AktorHooks {
                        intervals: vec![AktorInterval {
                            every: Duration::from_secs(5),
                            run: (async move |_: &mut ()| {
                                count.set(count.get() + 1);
                                notify.notify_one();
                            })
                            .into(),
                        }],
                        before_each: None,
                        after_each: None,
                    },
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
            .await
            .unwrap();

            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(4)).await;
            tokio::task::yield_now().await;
            assert_eq!(ticks.get(), 0);
            tokio::time::advance(Duration::from_secs(1)).await;
            ticked.notified().await;
            assert_eq!(ticks.get(), 1);

            let mut shutdown = core::future::IntoFuture::into_future(actors.shutdown());

            assert!((&mut shutdown).now_or_never().is_none());
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(3)).await;
            assert!((&mut shutdown).now_or_never().is_none());
            assert_eq!(cleaned.get(), 0);
            tokio::time::advance(Duration::from_secs(1)).await;

            let report = shutdown.await;

            assert!(!report.failed(), "{report}");
            assert_eq!(cleaned.get(), 1);
        })
        .await;
}

#[tokio::test]
async fn local_deadline() {
    use futures_util::FutureExt;

    struct Clock;
    impl local::clock::AktorGroupClock for Clock {
        type Deadline = ();

        fn deadline(_: Duration) {}

        fn wait(_: ()) -> message::LocalFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    let mut group = local::AktorGroup::<Clock>::new();
    let completion = group.completion();

    assert!(completion.try_report().is_none());

    let driver = group
        .listen_with(async |_| {
            core::future::pending::<()>().await;
            Ok::<_, String>(())
        })
        .unwrap();

    let _handle = group
        .spawn_args::<u32, 1, ()>(AktorArgs {
            name: "pending setup".into(),
            capacity: 1,
            setup: async || {
                core::future::pending::<()>().await;
                Ok(0)
            },
            cleanup: async |_| Ok(()),
        })
        .unwrap();

    let mut driver = Box::pin(driver);

    assert!((&mut driver).now_or_never().is_none());
    group.killswitch().stop();

    let report = driver.await;

    assert!(report.ran_past_grace);
    assert!(completion.try_report().unwrap().ran_past_grace);
    assert!(group.completion().await.ran_past_grace);
}

#[tokio::test]
async fn local_driver_loss() {
    {
        use futures_util::FutureExt;

        struct HookError;
        impl Drop for HookError {
            fn drop(&mut self) {
                panic!("local final hook data drop died");
            }
        }

        for error in [false, true] {
            let mut group = local::AktorGroup::<local::clock::Tokio>::new();
            let driver = group
                .listen_with(async move |_| {
                    if error {
                        let _resource = HookError;
                        Err(String::from("local final hook failed"))
                    } else {
                        std::panic::panic_any(HookError)
                    }
                })
                .unwrap();

            group.killswitch().stop();

            let report = match std::panic::AssertUnwindSafe(driver).catch_unwind().await {
                Ok(report) => report,
                Err(payload) => {
                    core::mem::forget(payload);
                    panic!("local final hook escaped shutdown");
                }
            };

            assert!(report.failed());

            assert_eq!(report.failure.as_ref().unwrap().phase, "cleanup");
            let sentinel = if error {
                "local final hook data drop died"
            } else {
                "panic payload had no message"
            };
            assert!(report.to_string().contains(sentinel));

            assert!(group.completion().await.failed());
        }
    }
    {
        struct UnpolledHook;
        impl Drop for UnpolledHook {
            fn drop(&mut self) {
                panic!("unpolled hook drop died");
            }
        }

        let mut group = local::AktorGroup::<local::clock::Tokio>::new();
        let driver = group.listen().unwrap();
        let completion = group.completion();
        let hook = UnpolledHook;
        let _handle = group
            .spawn_with_hooks(
                AktorArgs::new(
                    "unpolled local owner",
                    async || core::future::pending::<Result<u32, AktorError>>().await,
                    async |_| Ok(()),
                ),
                local::hooks::CallHooks {
                    before_each: Some(Box::new(move |_: &mut u32, _: operation::Operation| {
                        let _keep = &hook;
                    })),
                    ..Default::default()
                },
                AktorExecution::Local,
            )
            .unwrap();

        let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(driver)));

        assert!(
            dropped.is_ok(),
            "owner destruction escaped the group driver"
        );

        let report = tokio::time::timeout(Duration::from_millis(100), completion)
            .await
            .unwrap();

        assert!(report.failed());
        assert!(
            report
                .application
                .iter()
                .any(|error| error.diagnostics.contains("unpolled hook"))
        );
    }

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let cleaned = Rc::new(Cell::new(false));
            let end = cleaned.clone();
            let failed_cleaned = Rc::new(Cell::new(false));
            let failed_end = failed_cleaned.clone();
            let first = AktorNew {
                name: "faulty local owner".into(),
                kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                    tokio::task::spawn_local(future);
                    Ok(())
                }),
                start: async || aktor_ready(Counter(Rc::new(Cell::new(0)))),
                end: async move |_: Counter| {
                    failed_end.set(true);
                    Ok::<_, String>(())
                },
                hooks: AktorHooks {
                    intervals: vec![],
                    before_each: Some(
                        (|_: &mut Counter, _: operation::Operation| panic!("local operation died"))
                            .into(),
                    ),
                    after_each: None,
                },
                capacity: 32,
                stuck: AktorStuck::Kill,
            };

            let second = AktorNew {
                name: "other local owner".into(),
                kind: AktorKind::Local::<local::clock::Tokio>(|_| {
                    Err(String::from("driver already started"))
                }),
                start: async || aktor_ready(Counter(Rc::new(Cell::new(0)))),
                end: async move |_: Counter| {
                    end.set(true);
                    Ok::<_, String>(())
                },
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            };

            let actors = aktor_start(AktorSetup {
                actors: (first, second),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
            .await
            .unwrap();
            let reply = add(&actors.handles.0, 1).send().await;
            let report = actors.completion().await;

            assert!(report.failed());
            assert!(
                failed_cleaned.get(),
                "the failing owner's cleanup was skipped"
            );
            assert!(
                cleaned.get(),
                "another owner's cleanup was cancelled by the panic"
            );
            assert!(
                report
                    .failure
                    .unwrap()
                    .message
                    .contains("local operation died")
            );
            drop(reply);
        })
        .await;

    let executor = tokio::task::LocalSet::new();
    let actors = executor
        .run_until(aktor_start(AktorSetup {
            actors: AktorNew {
                name: "local counter".into(),
                kind: AktorKind::TokioLocal(&executor),
                start: async || aktor_ready(Counter(Rc::new(Cell::new(0)))),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        }))
        .await
        .unwrap();

    drop(executor);

    let report = tokio::time::timeout(Duration::from_secs(1), actors.completion())
        .await
        .unwrap();

    assert!(report.failed());
    assert!(!report.ran_past_grace);

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let actors = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: "local failure".into(),
                    kind: AktorKind::TokioLocal(&executor),
                    start: async || aktor_ready(Counter(Rc::new(Cell::new(0)))),
                    end: (),
                    hooks: AktorHooks {
                        intervals: vec![],
                        before_each: Some(
                            (|_: &mut Counter, _: operation::Operation| {
                                panic!("before died");
                            })
                            .into(),
                        ),
                        after_each: None,
                    },
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
            .await
            .unwrap();

            let report = tokio::select! {
                report = actors.completion() => report,
                _ = add(&actors.handles, 1) => panic!("failed actor replied"),
            };
            let failure = report.failure.unwrap();

            assert!(failure.message.contains("before died"));

            struct Hook;
            impl Drop for Hook {
                fn drop(&mut self) {
                    panic!("hook drop died");
                }
            }

            for cleanup_fails in [false, true] {
                let hook = Hook;
                let actors = aktor_start(AktorSetup {
                    actors: AktorNew {
                        name: "local hook drop".into(),
                        kind: AktorKind::TokioLocal(&executor),
                        start: async || aktor_ready(85_u32),
                        end: async move |_: u32| {
                            if cleanup_fails {
                                Err(String::from("primary cleanup failed"))
                            } else {
                                Ok::<_, String>(())
                            }
                        },
                        hooks: AktorHooks {
                            intervals: vec![],
                            before_each: Some(
                                (move |_: &mut u32, _: operation::Operation| {
                                    let _keep = &hook;
                                })
                                .into(),
                            ),
                            after_each: None,
                        },
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                })
                .await
                .unwrap();

                let (_input, mut output) = user_count::latest(&actors.handles);
                let report = actors.shutdown().await;

                assert!(report.failed());

                let failure = report.failure.unwrap();

                assert!(failure.message.contains(if cleanup_fails {
                    "primary cleanup failed"
                } else {
                    "hook drop died"
                }));
                use futures_util::FutureExt;
                assert!(output.next().now_or_never().is_none());
            }
        })
        .await;
}
