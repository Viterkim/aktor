use super::*;

#[cfg(feature = "local")]
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
                        name: AktorName::new("custom counter"),
                        role: AktorNoRole,
                        kind: AktorKind::Custom(
                            local::clock::Tokio,
                            move |future| {
                                spawn.spawn_local(future);
                                Ok(())
                            },
                            async move |mut runner: AktorRunner<'_, Counter>| {
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
                        closures: AktorClosures {
                            start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                            end: Some(
                                (async move |state: Counter| {
                                    count.set(count.get() + 1);
                                    end.set(Some(state.0.get()));
                                    Ok(())
                                })
                                .into(),
                            ),
                            intervals: vec![],
                            before_each: Some(
                                (move |_: &mut Counter, _: operation::Operation| {
                                    log.set(log.get() + 1);
                                })
                                .into(),
                            ),
                            after_each: None,
                        },
                        options: AktorNewOptions { capacity: 32 },
                    },
                    shutdown: |_| {},
                    options: AktorOptions {
                        shutdown_grace: Duration::from_millis(100),
                    },
                })
                .await
                .unwrap();

                if mode == 3 {
                    entered.notified().await;

                    let report = actors.shutdown().await;

                    assert!(report.timed_out, "{report}");
                    assert_eq!(ended.get(), None);
                    assert!(
                        report
                            .application
                            .iter()
                            .any(|error| error.to_string().contains("pending runner drop"))
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
                    assert_eq!(report.actors[0].kind, Some(AktorExecution::Custom));
                    assert_eq!(output.next().await, Some(Ok(7)));
                    assert!(output.next().await.is_none());
                    drop(input);
                }

                assert_eq!(cleanup_calls.get(), u32::from(mode != 3));
            }
        })
        .await;
}

#[cfg(feature = "local")]
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
                async move |state| {
                    assert_eq!(state, 7);
                    cleanup.set(cleanup.get() + 1);

                    match cleanup_failure {
                        1 => Err(AktorCleanupError::new("cleanup refused")),
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
                    diagnostics[0].to_string(),
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
            async move |state| {
                assert_eq!(state, 7);
                cleanup.set(cleanup.get() + 1);

                match cleanup_failure {
                    1 => Err(AktorCleanupError::new("cleanup refused")),
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

#[cfg(feature = "local")]
#[tokio::test]
async fn local() {
    let executor = tokio::task::LocalSet::new();
    let count = Rc::new(Cell::new(0));
    let before = count.clone();
    let finished = Rc::new(Cell::new(0));
    let end = finished.clone();
    let thread = |name: &str| AktorNew {
        name: AktorName::new(name),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok::<_, AktorSetupError>(85_u32),
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: Default::default(),
    };

    let actors = executor
        .run_until(aktor_start(AktorSetup {
            actors: (
                thread("first"),
                AktorNew {
                    name: AktorName::new("local counter"),
                    role: aktors::Users,
                    kind: AktorKind::TokioLocal(&executor),
                    closures: AktorClosures {
                        start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                        end: Some(
                            (async move |counter: Counter| {
                                end.set(counter.0.get());
                                Ok(())
                            })
                            .into(),
                        ),
                        before_each: Some(
                            (move |_: &mut Counter, _: operation::Operation| {
                                before.set(before.get() + 1);
                            })
                            .into(),
                        ),
                        intervals: vec![],
                        after_each: None,
                    },
                    options: AktorNewOptions { capacity: 32 },
                },
                thread("last"),
            ),
            shutdown: |_| {},
            options: AktorOptions {
                shutdown_grace: Duration::from_secs(5),
            },
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
                    name: AktorName::new("supplied executor"),
                    role: aktors::Users,
                    kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                        tokio::task::spawn_local(future);
                        Ok(())
                    }),
                    closures: AktorClosures {
                        start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                        end: None,
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

            assert_eq!(local_add(&actors.handles, 85).await, Ok(85));

            let (sender, mut results) = local_add::latest(&actors.handles);

            sender.send(1);
            assert_eq!(results.next().await, Some(Ok(86)));

            let report = actors.shutdown().await;

            assert!(!report.failed(), "{report}");
            assert_eq!(report.actors[0].kind, Some(AktorExecution::Local));
        })
        .await;
}

#[cfg(feature = "local")]
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
                    name: AktorName::new("virtual time"),
                    role: AktorNoRole,
                    kind: AktorKind::Local::<Tokio>(|future| {
                        tokio::task::spawn_local(future);
                        Ok(())
                    }),
                    closures: AktorClosures {
                        start: async || Ok(()),
                        end: Some(
                            (async move |_: ()| {
                                tokio::time::sleep(Duration::from_secs(4)).await;
                                end.set(end.get() + 1);
                                Ok(())
                            })
                            .into(),
                        ),
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
                    options: AktorNewOptions { capacity: 32 },
                },
                shutdown: |_| {},
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
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

            let mut shutdown = Box::pin(actors.shutdown());

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

#[cfg(feature = "local")]
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
            Ok::<_, AktorError>(())
        })
        .unwrap();

    let _handle = group
        .spawn::<u32, 1, ()>(ActorArgs {
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

    assert!(report.timed_out);
    assert!(completion.try_report().unwrap().timed_out);
    assert!(group.completion().await.timed_out);
}

#[cfg(feature = "local")]
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
                        Err(AktorCleanupError {
                            diagnostics: "local final hook failed".into(),
                            data: HookError,
                        })
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

            if error {
                assert!(
                    report
                        .application
                        .iter()
                        .any(|error| error.to_string().contains("local final hook failed"))
                );
            } else {
                assert_eq!(report.failure.unwrap().phase, "cleanup");
            }

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
                ActorArgs::new(
                    "unpolled local owner",
                    async || core::future::pending::<Result<u32, AktorSetupError>>().await,
                    async |_| Ok(()),
                ),
                local::hooks::AktorHooks {
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
                .any(|error| error.to_string().contains("unpolled hook"))
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
                name: AktorName::new("faulty local owner"),
                role: AktorNoRole,
                kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                    tokio::task::spawn_local(future);
                    Ok(())
                }),
                closures: AktorClosures {
                    start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                    end: Some(
                        (async move |_: Counter| {
                            failed_end.set(true);
                            Ok(())
                        })
                        .into(),
                    ),
                    intervals: vec![],
                    before_each: Some(
                        (|_: &mut Counter, _: operation::Operation| panic!("local operation died"))
                            .into(),
                    ),
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            };

            let second = AktorNew {
                name: AktorName::new("other local owner"),
                role: AktorNoRole,
                kind: AktorKind::Local::<local::clock::Tokio>(|_| {
                    Err(AktorError::new("driver already started"))
                }),
                closures: AktorClosures {
                    start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                    end: Some(
                        (async move |_: Counter| {
                            end.set(true);
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

            let actors = aktor_start(AktorSetup {
                actors: aktor_setups! { first, second },
                shutdown: |_| {},
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
            })
            .await
            .unwrap();
            let reply = add(&actors.handles.first, 1).send().await;
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
                name: AktorName::new("local counter"),
                role: AktorNoRole,
                kind: AktorKind::TokioLocal(&executor),
                closures: AktorClosures {
                    start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                    end: None,
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
        }))
        .await
        .unwrap();

    drop(executor);

    let report = tokio::time::timeout(Duration::from_secs(1), actors.completion())
        .await
        .unwrap();

    assert!(report.failed());
    assert_eq!(report.actors[0].actor, "local counter");
    assert_eq!(report.actors[0].kind, Some(AktorExecution::TokioLocal));
    assert!(!report.timed_out);

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let actors = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: AktorName::new("local failure"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioLocal(&executor),
                    closures: AktorClosures {
                        start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                        end: None,
                        intervals: vec![],
                        before_each: Some(
                            (|_: &mut Counter, _: operation::Operation| {
                                panic!("before died");
                            })
                            .into(),
                        ),
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

            let report = tokio::select! {
                report = actors.completion() => report,
                _ = add(&actors.handles, 1) => panic!("failed actor replied"),
            };
            let failure = report.failure.unwrap();

            assert!(failure.message.contains("before died"));
            assert_eq!(failure.kind, Some(AktorExecution::TokioLocal));

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
                        name: AktorName::new("local hook drop"),
                        role: aktors::Users,
                        kind: AktorKind::TokioLocal(&executor),
                        closures: AktorClosures {
                            start: async || Ok(85_u32),
                            end: Some(
                                (async move |_: u32| {
                                    if cleanup_fails {
                                        Err(AktorCleanupError::new("primary cleanup failed"))
                                    } else {
                                        Ok(())
                                    }
                                })
                                .into(),
                            ),
                            intervals: vec![],
                            before_each: Some(
                                (move |_: &mut u32, _: operation::Operation| {
                                    let _keep = &hook;
                                })
                                .into(),
                            ),
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
