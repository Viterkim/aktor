use aktor::*;
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

struct Counter(Rc<Cell<u32>>);

#[test]
fn caller_runtime() {
    let Ok(case) = std::env::var("AKTOR_CHILD") else {
        for case in ["stop", "drop", "live"] {
            let output = super::support::child("setup::caller_runtime", case);
            assert!(
                output.status.success(),
                "{case}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        return;
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (entered, running) = std::sync::mpsc::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (cleaned, cleanup) = std::sync::mpsc::channel();
    let (finish, finishing) = tokio::sync::oneshot::channel();
    let mut finishing = Some(finishing);
    let caller_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let interval_closed = caller_closed.clone();
    let (ticked, ticks) = std::sync::mpsc::channel();
    let actors = runtime
        .block_on(start(AktorSetup {
            name: AktorName::new("caller runtime"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                intervals: if case == "live" {
                    vec![AktorInterval {
                        every: Duration::from_millis(1),
                        run: (async move |state: &mut usize| {
                            if interval_closed.load(std::sync::atomic::Ordering::Acquire) {
                                let _sent = ticked.send(*state);
                            }
                        })
                        .into(),
                    }]
                } else {
                    Vec::new()
                },
                end: Some(
                    (async move |state: usize| {
                        cleaned.send(state).unwrap();
                        finishing.take().unwrap().await.unwrap();
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        Ok(())
                    })
                    .into(),
                ),
                ..AktorClosures::new(async || Ok(0usize))
            },
            options: Some(AktorOptions {
                shutdown_grace: Duration::from_secs(1),
                ..Default::default()
            }),
        }))
        .unwrap();
    let handle = actors.handles.new_handle();
    let completion = actors.completion();
    let owner = actors.handles.completion();

    let reply = runtime.block_on(
        message::call_async(
            &handle,
            async move |state: &mut usize, ()| {
                entered.send(()).unwrap();
                released.await.unwrap();
                *state += 1;
            },
            (),
        )
        .send(),
    );
    running.recv_timeout(Duration::from_secs(1)).unwrap();
    drop(reply);

    if case == "stop" {
        let _closing = actors.shutdown();
    }

    drop(runtime);
    caller_closed.store(true, std::sync::atomic::Ordering::Release);
    release.send(()).unwrap();

    if case == "live" {
        assert_eq!(ticks.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    }

    drop(actors);
    assert_eq!(cleanup.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
    finish.send(()).unwrap();

    let report = executor::block_on(completion.wait());
    assert!(!report.failed(), "{report}");
    assert!(executor::block_on(owner.wait()).is_ok());
    assert_eq!(report.actors.len(), 1);
    drop(handle);
}

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
                let actors = start(AktorSetup {
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
                    options: Some(AktorOptions {
                        shutdown_grace: Duration::from_millis(100),
                        ..Default::default()
                    }),
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

mod aktors {
    pub struct Users;
    pub struct Archive;
}

#[aktor(role = aktors::Users)]
async fn user_count(counter: &u32) -> u32 {
    *counter
}

#[aktor(role = aktors::Archive)]
async fn archive_count<T: From<u32>>(counter: &u32) -> T {
    T::from(*counter)
}

#[cfg(feature = "local")]
#[aktor(role = aktors::Users)]
async fn local_add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    add(counter, by).await
}

#[aktor]
async fn add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    if by == 0 {
        return Err("give me something");
    }

    let value = counter.0.get() + by;

    counter.0.set(value);
    Ok(value)
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
            .try_cast()
            .unwrap();

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

        crash(&handle).try_cast().unwrap();

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

#[tokio::test]
async fn counter() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let before = log.clone();
    let after = log.clone();
    let end = log.clone();
    let setup = AktorSetup {
        name: AktorName::new("BingoManden"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok(Counter(Rc::new(Cell::new(0)))),
            end: Some(
                (async move |counter: Counter| {
                    end.lock().unwrap().push(("end", counter.0.get()));
                    Ok(())
                })
                .into(),
            ),
            intervals: vec![],
            before_each: Some(
                (move |counter: &mut Counter, operation: operation::Operation| {
                    assert!(operation.name.contains("add"));
                    before.lock().unwrap().push(("before", counter.0.get()));
                })
                .into(),
            ),
            after_each: Some(
                (move |counter: &mut Counter, _: operation::Operation| {
                    after.lock().unwrap().push(("after", counter.0.get()));
                })
                .into(),
            ),
        },
        options: None,
    };

    assert_eq!(setup.execution(), AktorExecution::TokioThread);

    let actors = tokio::spawn(setup.start()).await.unwrap().unwrap();
    let completion = actors.completion();

    assert!(completion.try_report().is_none());
    assert_eq!(add(&actors.handles, 5).await, Ok(5));
    assert_eq!(add(&actors.handles, 0).await, Err("give me something"));

    let (input, mut output) = add::latest(&actors.handles);

    input.send(3);
    assert_eq!(output.next().await, Some(Ok(8)));

    actors.handles.actor.pause().await.unwrap();
    actors
        .handles
        .actor
        .resume(|| Ok(Counter(Rc::new(Cell::new(10)))))
        .await
        .unwrap();

    assert_eq!(add(&actors.handles, 2).await, Ok(12));

    let closing = actors.shutdown();
    let stopping = actors.killswitch().is_stopping();

    drop(closing);

    let report = actors.shutdown().await;

    assert!(
        stopping,
        "shutdown was deferred until its observer was polled"
    );
    assert!(!report.failed(), "{report}");
    assert_eq!(
        completion.try_report().unwrap().actors.len(),
        report.actors.len()
    );
    assert!(!completion.try_report().unwrap().failed());
    assert_eq!(report.actors[0].actor, "BingoManden");
    assert_eq!(report.actors[0].kind, Some(AktorExecution::TokioThread));
    assert_eq!(
        *log.lock().unwrap(),
        [
            ("before", 0),
            ("after", 5),
            ("before", 5),
            ("after", 5),
            ("before", 5),
            ("after", 8),
            ("end", 8),
            ("before", 10),
            ("after", 12),
            ("end", 12),
        ]
    );
}

#[tokio::test]
async fn intervals() {
    let (saved, backup) = tokio::sync::oneshot::channel();
    let mut saved = Some(saved);
    let mut destination = rusqlite::Connection::open_in_memory().unwrap();
    let actors = start(AktorSetup {
        name: AktorName::new("SQLite backup"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            intervals: vec![AktorInterval {
                every: Duration::from_millis(1),
                run: (async move |db: &mut rusqlite::Connection| {
                    let result =
                        (|| {
                            rusqlite::backup::Backup::new(db, &mut destination)?
                                .run_to_completion(5, Duration::ZERO, None)?;
                            destination.query_row("SELECT value FROM saved", [], |row| {
                                row.get::<_, u32>(0)
                            })
                        })();
                    if let Some(saved) = saved.take() {
                        let _result = saved.send(result);
                    }
                })
                .into(),
            }],
            ..AktorClosures::new(async || {
                let db = rusqlite::Connection::open_in_memory()
                    .map_err(|error| AktorSetupError::new(error.to_string()))?;

                db.execute_batch("CREATE TABLE saved(value); INSERT INTO saved VALUES(85)")
                    .map_err(|error| AktorSetupError::new(error.to_string()))?;
                Ok(db)
            })
        },
        options: None,
    })
    .await
    .unwrap();

    assert_eq!(backup.await.unwrap().unwrap(), 85);
    assert!(!actors.shutdown().await.failed());

    struct IntervalDrop;
    impl Drop for IntervalDrop {
        fn drop(&mut self) {
            panic!("interval drop died");
        }
    }

    let observed = Arc::new(Notify::new());
    let notify = observed.clone();
    let actors = start(AktorSetup {
        name: AktorName::new("Haandboldfuglen"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok(Counter(Rc::new(Cell::new(0)))),
            end: None,
            intervals: vec![AktorInterval {
                every: Duration::from_millis(1),
                run: (async move |counter: &mut Counter| {
                    counter.0.set(85);
                    notify.notify_one();
                    std::future::pending::<()>().await;
                })
                .into(),
            }],
            before_each: None,
            after_each: None,
        },
        options: Some(AktorOptions {
            shutdown_grace: Duration::from_millis(200),
            ..Default::default()
        }),
    })
    .await
    .unwrap();

    observed.notified().await;

    let report = actors.shutdown().await;

    assert!(report.timed_out);

    #[cfg(feature = "local")]
    {
        let executor = tokio::task::LocalSet::new();

        executor
            .run_until(async {
                let observed = Arc::new(Notify::new());
                let notify = observed.clone();
                let actors = start(AktorSetup {
                    name: AktorName::new("local interval"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioLocal(&executor),
                    closures: AktorClosures {
                        start: async || Ok(0_u32),
                        end: None,
                        intervals: vec![AktorInterval {
                            every: Duration::from_millis(1),
                            run: (async move |_: &mut u32| {
                                notify.notify_one();
                                std::future::pending::<()>().await;
                            })
                            .into(),
                        }],
                        before_each: None,
                        after_each: None,
                    },
                    options: Some(AktorOptions {
                        shutdown_grace: Duration::from_millis(200),
                        ..Default::default()
                    }),
                })
                .await
                .unwrap();

                observed.notified().await;

                let report = actors.shutdown().await;

                assert!(report.timed_out);
                assert!(report.failure.is_none(), "{report}");

                let interval = IntervalDrop;
                let actors = start(AktorSetup {
                    name: AktorName::new("local idle interval"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioLocal(&executor),
                    closures: AktorClosures {
                        start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                        end: None,
                        intervals: vec![AktorInterval {
                            every: Duration::from_secs(3600),
                            run: (async move |_: &mut Counter| {
                                let _keep = &interval;
                            })
                            .into(),
                        }],
                        before_each: None,
                        after_each: None,
                    },
                    options: None,
                })
                .await
                .unwrap();

                let (_input, mut output) = add::latest(&actors.handles);
                let report = actors.shutdown().await;

                assert!(report.failed(), "{report}");
                assert_eq!(report.failure.unwrap().actor, "local idle interval");
                use futures_util::FutureExt;
                assert!(output.next().now_or_never().is_none());
            })
            .await;
    }

    let interval = IntervalDrop;
    let actors = start(AktorSetup {
        name: AktorName::new("idle interval"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok(0u32),
            end: None,
            intervals: vec![AktorInterval {
                every: Duration::from_secs(3600),
                run: (async move |_: &mut u32| {
                    let _keep = &interval;
                })
                .into(),
            }],
            before_each: None,
            after_each: None,
        },
        options: None,
    })
    .await
    .unwrap();

    let report = actors.shutdown().await;

    assert!(report.failed(), "{report}");
    assert_eq!(report.failure.unwrap().actor, "idle interval");
}

#[tokio::test]
async fn multiple() {
    fn counter(name: &str, fail: bool, cleaned: Arc<Notify>) -> impl setup::AktorStart {
        AktorSetup {
            name: AktorName::new(name),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async move || {
                    if fail {
                        Err(AktorSetupError::new("no thanks"))
                    } else {
                        Ok(Counter(Rc::new(Cell::new(0))))
                    }
                },
                end: Some(
                    (async move |_: Counter| {
                        cleaned.notify_one();
                        Ok(())
                    })
                    .into(),
                ),
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: None,
        }
    }

    let cleaned = Arc::new(Notify::new());
    let result = start(aktor_setups! {
        ready: counter("BingoManden", false, cleaned.clone()),
        failed: counter("Haandboldfuglen", true, Arc::new(Notify::new())),
    })
    .await;

    assert!(result.is_err());
    cleaned.notified().await;

    let actors = tokio::spawn(start((
        AktorSetup {
            name: AktorName::new(format!("users-{}", 1)),
            role: aktors::Users,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok(1_u32),
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: None,
        },
        AktorSetup {
            name: AktorName::new("two"),
            role: aktors::Archive,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok(85_u32),
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: None,
        },
    )))
    .await
    .unwrap()
    .unwrap();

    assert_eq!(user_count(&actors.handles.0).await, 1);
    assert_eq!(archive_count::<u32, _>(&actors.handles.1).await, 85);

    let (input, mut output) = user_count::latest(&actors.handles.0);

    input.send();
    assert_eq!(output.next().await, Some(1));
    assert!(!actors.shutdown().await.failed());
}

async fn close_interval(waiting: bool) {
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let performed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let end = cleaned.clone();
    let actors = start(aktor_setups! {
        timed: AktorSetup {
            name: AktorName::new("interval owner"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                end: Some((async move |_: u32| {
                    end.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                }).into()),
                intervals: vec![AktorInterval {
                    every: Duration::from_secs(1),
                    run: (async |value: &mut u32| { *value += 1; }).into(),
                }],
                ..AktorClosures::new(async || Ok(0_u32))
            },
            options: Some(AktorOptions { capacity: 1, ..Default::default() }),
        },
        sibling: AktorSetup {
            name: AktorName::new("sibling"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures::new(async || Ok(0_u32)),
            options: None,
        },
    })
    .await
    .unwrap();

    tokio::task::yield_now().await;
    tokio::time::pause();

    let awake = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });

    let entered = Arc::new(Notify::new());
    let gate = Arc::new(Notify::new());

    if waiting {
        let signal = entered.clone();
        let release = gate.clone();
        let count = performed.clone();

        drop(
            message::call_async(
                &actors.handles.timed.handle,
                async move |_: &mut u32, ()| {
                    signal.notify_one();
                    release.notified().await;
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                },
                (),
            )
            .send()
            .await,
        );

        entered.notified().await;

        let count = performed.clone();
        let queued = message::call(
            &actors.handles.timed.handle,
            move |_: &mut u32, ()| {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            (),
        )
        .try_send();

        if queued.is_err() {
            gate.notify_one();
        }

        drop(queued.unwrap());
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let completed = actors.handles.timed.shutdown();

    gate.notify_one();
    completed.await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    awake.abort();
    assert!(
        !actors.killswitch().is_stopping(),
        "interval outlived its actor"
    );
    assert_eq!(
        message::call(&actors.handles.sibling.handle, |value, ()| *value, ()).await,
        0
    );
    assert!(!actors.shutdown().await.failed());
    assert_eq!(cleaned.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        performed.load(std::sync::atomic::Ordering::Relaxed),
        if waiting { 2 } else { 0 }
    );
}

#[tokio::test]
async fn interval_close() {
    close_interval(false).await;
}

#[tokio::test]
async fn interval_admission() {
    close_interval(true).await;
}

#[cfg(feature = "local")]
fn local_interval<Kind>(
    kind: Kind,
    timed: bool,
    cleaned: Arc<std::sync::atomic::AtomicUsize>,
) -> AktorSetup<u32, impl core::ops::AsyncFnOnce() -> Result<u32, AktorSetupError> + 'static, Kind>
where
    Kind: setup::kind::AktorMode<
            Each<u32> = dyn FnMut(&mut u32, operation::Operation),
            End<u32> = dyn setup::AktorEnd<u32>,
            Interval<u32> = dyn setup::AktorIntervalLogic<u32>,
        >,
{
    AktorSetup {
        name: AktorName::new(if timed { "interval owner" } else { "sibling" }),
        role: AktorNoRole,
        kind,
        closures: AktorClosures {
            end: Some(
                (async move |_: u32| {
                    cleaned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                })
                .into(),
            ),
            intervals: if timed {
                vec![AktorInterval {
                    every: Duration::from_secs(1),
                    run: (async |value: &mut u32| {
                        *value += 1;
                    })
                    .into(),
                }]
            } else {
                vec![]
            },
            ..AktorClosures::new(async || Ok(0_u32))
        },
        options: Some(AktorOptions {
            capacity: 1,
            ..Default::default()
        }),
    }
}

#[cfg(feature = "local")]
struct IntervalClock;
#[cfg(feature = "local")]
impl local::clock::AktorGroupClock for IntervalClock {
    type Deadline = tokio::time::Instant;

    fn deadline(duration: Duration) -> Self::Deadline {
        tokio::time::Instant::now() + duration
    }

    fn wait(deadline: Self::Deadline) -> message::LocalFuture<'static, ()> {
        Box::pin(tokio::time::sleep_until(deadline))
    }
}

#[cfg(feature = "local")]
type LocalCounter<Clock> = local::Handle<u32, 0, (), (), Clock>;

#[cfg(feature = "local")]
async fn close_local_interval<Group: setup::group::AktorSetupGroup, Clock>(
    actors: AktorStarted<(LocalCounter<Clock>, LocalCounter<Clock>), Group>,
    cleaned: Arc<std::sync::atomic::AtomicUsize>,
    waiting: bool,
) {
    use futures_util::FutureExt;

    tokio::task::yield_now().await;
    tokio::time::pause();

    let awake = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });

    let entered = Arc::new(Notify::new());
    let gate = Arc::new(Notify::new());
    let performed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let operation = operation::Operation {
        name: "held",
        caller: std::panic::Location::caller(),
    };

    if waiting {
        let signal = entered.clone();
        let release = gate.clone();
        let count = performed.clone();

        drop(
            local::Request::new(
                &actors.handles.0,
                operation,
                async move |_: &mut u32, ()| {
                    signal.notify_one();
                    release.notified().await;
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                },
                (),
            )
            .send()
            .await,
        );

        entered.notified().await;

        let count = performed.clone();
        let queued = local::Request::new(
            &actors.handles.0,
            operation,
            async move |value: &mut u32, ()| {
                *value += 1;
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            (),
        )
        .try_send();

        if queued.is_err() {
            gate.notify_one();
        }

        drop(queued.unwrap());
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let completed = actors.handles.0.shutdown();

    gate.notify_one();
    completed.await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    awake.abort();
    assert!(!Group::is_stopping(&actors.killswitch()));
    assert_eq!(
        local::Request::new(
            &actors.handles.1,
            operation,
            async |value: &mut u32, ()| *value,
            (),
        )
        .await,
        0
    );

    let probe = local::Request::new(
        &actors.handles.0,
        operation,
        async |value: &mut u32, ()| *value,
        (),
    );

    assert!(probe.now_or_never().is_none());
    assert!(
        Group::is_stopping(&actors.killswitch()),
        "ordinary lost calls stopped reporting failure"
    );

    let report = actors.shutdown().await;

    assert_eq!(report.failure.unwrap().actor, "interval owner");
    assert_eq!(cleaned.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        performed.load(std::sync::atomic::Ordering::Relaxed),
        if waiting { 2 } else { 0 }
    );
}

#[cfg(feature = "local")]
#[tokio::test]
async fn interval_local_close() {
    let executor = Rc::new(tokio::task::LocalSet::new());

    executor
        .run_until(async {
            for driven in [false, true] {
                for waiting in [false, true] {
                    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let sibling = Arc::new(std::sync::atomic::AtomicUsize::new(0));

                    if driven {
                        let make_kind = || {
                            let executor = executor.clone();

                            AktorKind::Local::<IntervalClock>(move |future| {
                                executor.spawn_local(future);
                                Ok(())
                            })
                        };

                        let actors = start((
                            local_interval(make_kind(), true, cleaned.clone()),
                            local_interval(make_kind(), false, sibling),
                        ))
                        .await
                        .unwrap();

                        close_local_interval(actors, cleaned, waiting).await;
                    } else {
                        let actors = start((
                            local_interval(AktorKind::TokioLocal(&executor), true, cleaned.clone()),
                            local_interval(AktorKind::TokioLocal(&executor), false, sibling),
                        ))
                        .await
                        .unwrap();

                        close_local_interval(actors, cleaned, waiting).await;
                    }
                }
            }
        })
        .await;
}

#[tokio::test]
async fn interval_pause() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    let observed = Arc::new(Notify::new());
    let notify = observed.clone();
    let runs = Arc::new(AtomicUsize::new(0));
    let count = runs.clone();
    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let end = cleaned.clone();
    let (release, gate) = oneshot::channel();
    let mut gate = Some(gate);
    let actors = start(AktorSetup {
        name: AktorName::new("interval counter"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok(0_u32),
            end: Some(
                (async move |state: u32| {
                    end.lock().unwrap().push(state);
                    Ok(())
                })
                .into(),
            ),
            intervals: vec![AktorInterval {
                every: Duration::from_secs(1),
                run: (async move |state: &mut u32| {
                    *state += 1;
                    count.fetch_add(1, Ordering::SeqCst);
                    notify.notify_one();

                    if let Some(gate) = gate.take() {
                        let _released = gate.await;
                    }
                })
                .into(),
            }],
            before_each: None,
            after_each: None,
        },
        options: None,
    })
    .await
    .unwrap();

    tokio::task::yield_now().await;
    tokio::time::pause();

    let awake = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });

    tokio::time::advance(Duration::from_millis(1001)).await;
    observed.notified().await;
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    release.send(()).unwrap();
    assert_eq!(
        message::call(&actors.handles.handle, |state, ()| *state, ()).await,
        1
    );
    actors.handles.actor.pause().await.unwrap();
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    actors.handles.actor.resume(|| Ok(100)).await.unwrap();
    observed.notified().await;
    assert_eq!(
        message::call(&actors.handles.handle, |state, ()| *state, ()).await,
        101
    );
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    tokio::time::resume();
    awake.abort();

    assert!(!actors.shutdown().await.failed());
    assert_eq!(*cleaned.lock().unwrap(), [1, 101]);
}

#[tokio::test]
async fn startup_cancel() {
    let entered = Arc::new(Notify::new());
    let beginning = entered.clone();
    let cleaned = Arc::new(Notify::new());
    let end = cleaned.clone();
    let options = || {
        Some(AktorOptions {
            shutdown_grace: Duration::from_secs(1),
            ..Default::default()
        })
    };

    let startup = start(aktor_setups! {
        ready: AktorSetup {
            name: AktorName::new("ready counter"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok(0_u32),
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
            options: options(),
        },
        waiting: AktorSetup {
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
            options: options(),
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

    assert!(report.timed_out);
    assert_eq!(report.actors.len(), 2);

    let startup = start(AktorSetup {
        name: AktorName::new("never started"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || panic!("stopped startup ran"),
            end: None::<AktorClosure<dyn setup::AktorEnd<u32> + Send>>,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    });

    let kill = startup.killswitch();
    let completion = startup.completion();

    kill.stop();
    assert!(startup.await.is_err());
    assert!(completion.await.actors.is_empty());

    let startup = start(AktorSetup {
        name: AktorName::new("discarded startup"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || panic!("discarded startup ran"),
            end: None::<AktorClosure<dyn setup::AktorEnd<u32> + Send>>,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
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
#[tokio::test]
async fn local() {
    let executor = tokio::task::LocalSet::new();
    let count = Rc::new(Cell::new(0));
    let before = count.clone();
    let finished = Rc::new(Cell::new(0));
    let end = finished.clone();
    let thread = |name: &str| AktorSetup {
        name: AktorName::new(name),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok(85_u32),
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    };

    let actors = executor
        .run_until(start((
            thread("first"),
            AktorSetup {
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
                    intervals: vec![],
                    before_each: Some(
                        (move |_: &mut Counter, _: operation::Operation| {
                            before.set(before.get() + 1);
                        })
                        .into(),
                    ),
                    after_each: None,
                },
                options: None,
            },
            thread("last"),
        )))
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

            let actors = start(AktorSetup {
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
                options: None,
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
            let actors = start(AktorSetup {
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
                options: None,
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
            let first = AktorSetup {
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
                options: None,
            };

            let second = AktorSetup {
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
                options: None,
            };

            let actors = start(aktor_setups! { first, second }).await.unwrap();
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
        .run_until(start(AktorSetup {
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
            options: None,
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
            let actors = start(AktorSetup {
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
                options: None,
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
                let actors = start(AktorSetup {
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
                    options: None,
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

#[tokio::test]
async fn named() {
    let task = |name: &str| AktorSetup {
        name: AktorName::new(name),
        role: aktors::Users,
        kind: AktorKind::TokioTask,
        closures: AktorClosures {
            start: async || Ok(85_u32),
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    };

    let setups = aktor_setups! {
        first: task("first"),
        second: task("second"),
        third: task("third"),
        fourth: task("fourth"),
        fifth: task("fifth"),
        sixth: task("sixth"),
        seventh: task("seventh"),
        eighth: task("eighth"),
        r#type: AktorSetup {
            name: AktorName::new("archive"),
            role: aktors::Archive,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok(7_u32),
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: None,
        },
    };

    let actors = start(setups).await.unwrap();

    assert_eq!(user_count(&actors.handles.first).await, 85);
    assert_eq!(user_count(&actors.handles.eighth).await, 85);
    assert_eq!(archive_count::<u32, _>(&actors.handles.r#type).await, 7);

    let report = actors.shutdown().await;

    assert_eq!(report.actors.len(), 9);
    assert!(!report.failed(), "{report}");
}

#[cfg(feature = "std_thread")]
#[test]
fn standard() {
    executor::block_on(async {
        let log = Arc::new(Mutex::new(Vec::new()));
        let before = log.clone();
        let after = log.clone();
        let end = log.clone();
        let actors = start(AktorSetup {
            name: AktorName::new("standard counter"),
            role: AktorNoRole,
            kind: AktorKind::StdThread,
            closures: AktorClosures {
                start: async || {
                    assert!(tokio::runtime::Handle::try_current().is_err());
                    Ok(Counter(Rc::new(Cell::new(0))))
                },
                end: Some(
                    (async move |counter: Counter| {
                        assert!(tokio::runtime::Handle::try_current().is_err());
                        end.lock().unwrap().push(("end", counter.0.get()));
                        Ok(())
                    })
                    .into(),
                ),
                intervals: vec![],
                before_each: Some(
                    (move |counter: &mut Counter, _: operation::Operation| {
                        before.lock().unwrap().push(("before", counter.0.get()));
                    })
                    .into(),
                ),
                after_each: Some(
                    (move |counter: &mut Counter, _: operation::Operation| {
                        after.lock().unwrap().push(("after", counter.0.get()));
                    })
                    .into(),
                ),
            },
            options: None,
        })
        .await
        .unwrap();

        assert_eq!(add(&actors.handles, 2).await, Ok(2));
        assert_eq!(add(&actors.handles, 0).await, Err("give me something"));

        let gate = Arc::new(Notify::new());
        let mut reply = held_add::request(&actors.handles, gate.clone())
            .send()
            .await;

        use futures_util::FutureExt;
        assert!(reply.timeout(Duration::MAX).now_or_never().is_none());
        assert!(
            reply
                .timeout(Duration::from_millis(5))
                .await
                .unwrap_err()
                .admitted
        );
        gate.notify_one();
        assert_eq!(reply.await, 3);

        let (input, mut output) = add::latest(&actors.handles);

        input.send(4);
        assert_eq!(output.next().await, Some(Ok(7)));
        input.send(1);

        let report = actors.shutdown().await;

        assert!(!report.failed(), "{report}");
        assert_eq!(report.actors[0].kind, Some(AktorExecution::StdThread));
        assert_eq!(output.next().await, Some(Ok(8)));
        assert_eq!(output.next().await, None);
        assert_eq!(log.lock().unwrap().last(), Some(&("end", 8)));
        drop(input);

        let actors = start(AktorSetup {
            name: AktorName::new("standard cleanup"),
            role: AktorNoRole,
            kind: AktorKind::StdThread,
            closures: AktorClosures {
                start: async || Ok(0u32),
                end: Some((async |_: u32| core::future::pending().await).into()),
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Some(AktorOptions {
                shutdown_grace: Duration::from_millis(200),
                ..Default::default()
            }),
        })
        .await
        .unwrap();

        assert!(actors.shutdown().await.timed_out);
    });
}

#[cfg(feature = "std_thread")]
#[aktor]
async fn held_add(counter: &mut Counter, gate: Arc<Notify>) -> u32 {
    counter.0.set(counter.0.get() + 1);
    gate.notified().await;
    counter.0.get()
}

#[cfg(feature = "local")]
#[tokio::test]
async fn stopping_startup() {
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
                        start(AktorSetup {
                            name: AktorName::new("driven startup"),
                            role: AktorNoRole,
                            kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                                tokio::task::spawn_local(future);
                                Ok(())
                            }),
                            closures: AktorClosures {
                                end: Some(cleanup.into()),
                                ..AktorClosures::new(setup)
                            },
                            options: None,
                        }),
                        entered,
                        release,
                    )
                    .await;
                } else {
                    stop(
                        start(AktorSetup {
                            name: AktorName::new("spawned startup"),
                            role: AktorNoRole,
                            kind: AktorKind::TokioLocal(&executor),
                            closures: AktorClosures {
                                end: Some(cleanup.into()),
                                ..AktorClosures::new(setup)
                            },
                            options: None,
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

#[cfg(feature = "embassy_cross_core")]
#[tokio::test]
async fn cleanup_primary() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    struct Hook;
    impl Drop for Hook {
        fn drop(&mut self) {
            panic!("hook drop died");
        }
    }

    let (handle, mut owner) = cross_core::channel::<u32>(1).unwrap();
    let hook = Hook;

    owner.hooks.before_each = Some(Box::new(move |_, _| {
        std::hint::black_box(&hook);
    }));
    drop(handle.shutdown());

    let result = AssertUnwindSafe(owner.run_with(
        async || Ok(7),
        async |_| {
            panic!("cleanup died");
            #[allow(unreachable_code)]
            Ok(())
        },
    ))
    .catch_unwind()
    .await
    .unwrap_err();

    assert_eq!(result.downcast_ref::<&str>(), Some(&"cleanup died"));
    assert!(handle.completion().wait().await.is_err());
}

struct QueuedDrop;
impl Drop for QueuedDrop {
    fn drop(&mut self) {
        panic!("queued input drop died");
    }
}

#[cfg(feature = "local")]
#[tokio::test]
async fn queued_drop_primary() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let (handle, owner) = local::channel::<u32, 2, ()>().unwrap();
    let operation = operation::Operation {
        name: "panic",
        caller: std::panic::Location::caller(),
    };

    local::Request::new(
        &handle,
        operation,
        async |_: &mut u32, ()| panic!("operation died"),
        (),
    )
    .try_cast()
    .unwrap();

    local::Request::new(
        &handle,
        operation,
        async |_: &mut u32, _: QueuedDrop| {},
        QueuedDrop,
    )
    .try_cast()
    .unwrap();

    let cleaned = Rc::new(Cell::new(0));
    let cleanup = cleaned.clone();
    let completion = owner.completion();
    let payload = AssertUnwindSafe(owner.run(7, async move |_| {
        cleanup.set(cleanup.get() + 1);
        Ok(())
    }))
    .catch_unwind()
    .await
    .unwrap_err();

    assert_eq!(payload.downcast_ref::<&str>(), Some(&"operation died"));
    assert_eq!(cleaned.get(), 1);
    assert!(completion.wait().await.is_err());
    #[cfg(feature = "embassy_cross_core")]
    {
        #[aktor]
        async fn crash(_: &mut u32) {
            panic!("operation died");
        }
        #[aktor]
        async fn discard(_: &mut u32, _: QueuedDrop) {}

        let (handle, owner) = cross_core::channel::<u32>(2).unwrap();

        crash(&handle).try_cast().unwrap();
        discard(&handle, QueuedDrop).try_cast().unwrap();

        let payload = AssertUnwindSafe(owner.run_with(async || Ok(7), async |_| Ok(())))
            .catch_unwind()
            .await
            .unwrap_err();

        assert_eq!(payload.downcast_ref::<&str>(), Some(&"operation died"));
        assert!(handle.completion().wait().await.is_err());
    }
}

#[tokio::test]
async fn existing_group() {
    let mut group = AktorGroup::with_grace(Duration::from_millis(200));
    let ended = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = ended.clone();
    let setup = AktorSetup {
        name: AktorName::new("prepared counter"),
        role: aktors::Users,
        kind: AktorKind::TokioThread,
        closures: AktorClosures::new(async || Ok(17_u32)),
        options: Some(AktorOptions {
            shutdown_grace: Duration::ZERO,
            ..Default::default()
        }),
    };

    let error = start_in(&group, setup).await.err().unwrap();

    assert!(error.report.is_none());
    assert!(!group.killswitch().is_stopping());

    let completion = group
        .start_with(async move |_| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, AktorCleanupError>(())
        })
        .unwrap();

    let handles = start_in(
        &group,
        aktor_setups! {
            first: AktorSetup {
                name: AktorName::new("first"),
                role: aktors::Users,
                kind: AktorKind::TokioThread,
                closures: AktorClosures::new(async || Ok(17_u32)),
                options: None,
            },
            second: AktorSetup {
                name: AktorName::new("second"),
                role: aktors::Users,
                kind: AktorKind::TokioTask,
                closures: AktorClosures::new(async || Ok(23_u32)),
                options: None,
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

#[cfg(feature = "std_thread")]
#[tokio::test]
async fn lifecycle_data() {
    use listener::DedicatedStartError;
    use setup::{AktorPairStartError, AktorThreadStartError};

    let mut group = AktorGroup::new();

    group.start().unwrap();

    let handle = AktorSetup {
        name: AktorName::new("typed counter"),
        role: aktors::Users,
        kind: AktorKind::TokioThread.with_data(),
        closures: AktorClosures {
            start: async || Ok::<_, AktorSetupError<Cell<u32>>>(17_u32),
            end: Some(
                (async |_: u32| {
                    Err(AktorCleanupError {
                        diagnostics: "cleanup data".into(),
                        data: vec![23_u32],
                    })
                })
                .into(),
            ),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    }
    .start_in(&group)
    .await
    .unwrap();

    assert_eq!(user_count(&handle).await, 17);

    let error = handle.shutdown().wait().await.unwrap_err();

    match &*error {
        owner::OwnerError::Cleanup(errors) => assert_eq!(errors.errors[0].data, [23]),
        error => panic!("unexpected completion: {error:?}"),
    }

    assert!(group.shutdown().await.failed());

    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = cleaned.clone();
    let result = start(aktor_setups! {
        ready: AktorSetup {
            name: AktorName::new("ready"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread.with_data::<Cell<u32>, ()>(),
            closures: AktorClosures {
                end: Some((async move |_: u32| {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(AktorCleanupError::new("rollback cleanup refused"))
                }).into()),
                ..AktorClosures::new(async || Ok(0_u32))
            },
            options: None,
        },
        failed: AktorSetup {
            name: AktorName::new("failed"),
            role: AktorNoRole,
            kind: AktorKind::StdThread.with_data::<String, ()>(),
            closures: AktorClosures::new(async || Err::<u32, _>(AktorSetupError {
                diagnostics: "setup data".into(),
                data: "storage failure".to_owned(),
            })),
            options: None,
        },
    })
    .await;

    match result {
        Err(AktorStartupError {
            error:
                AktorPairStartError::Second(AktorThreadStartError::Thread(DedicatedStartError::Init(
                    error,
                ))),
            report: Some(report),
        }) => {
            assert_eq!(error.data, "storage failure");
            assert!(!report.timed_out, "{report}");
            assert_eq!(
                report.to_string().matches("setup data").count(),
                1,
                "{report}"
            );

            let ready = report
                .actors
                .iter()
                .find(|actor| actor.actor == "ready")
                .unwrap();

            assert_eq!(ready.diagnostics.len(), 1, "{report}");
            assert!(
                ready.diagnostics[0]
                    .to_string()
                    .contains("rollback cleanup refused")
            );
        }
        _ => panic!("typed setup failure was lost"),
    }

    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn existing_group_cancel() {
    let mut group = AktorGroup::with_grace(Duration::from_secs(1));
    let completion = group.start().unwrap();
    let previous_cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let previous = previous_cleaned.clone();
    let _healthy = start_in(
        &group,
        AktorSetup {
            name: AktorName::new("existing healthy actor"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                end: Some(
                    (async move |_: ()| {
                        previous.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(())
                    })
                    .into(),
                ),
                ..AktorClosures::new(async || Ok(()))
            },
            options: None,
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
    let setup = AktorSetup {
        name: AktorName::new("cancelled prepared counter"),
        role: aktors::Users,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async move || {
                signal.notify_one();
                gate.notified().await;
                Ok(17_u32)
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
        options: Some(AktorOptions {
            shutdown_grace: Duration::ZERO,
            ..Default::default()
        }),
    };

    let mut startup = Box::pin(start_in(&group, setup));

    tokio::select! {
        _ = &mut startup => panic!("setup passed its gate"),
        _ = entered.notified() => {},
    }
    drop(startup);
    assert!(group.killswitch().is_stopping());
    release.notify_one();

    let report = completion.await;

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
async fn existing_local_group() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let mut group = local::AktorGroup::<local::clock::Tokio>::new();
            let driver = executor.spawn_local(group.listen().unwrap());
            let actors = start_in(
                &group,
                AktorSetup {
                    name: AktorName::new("existing local counter"),
                    role: aktors::Users,
                    kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                        tokio::task::spawn_local(future);
                        Ok(())
                    }),
                    closures: AktorClosures::new(async || Ok(17_u32)),
                    options: None,
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
            let error = start_in(
                &group,
                (
                    AktorSetup {
                        name: AktorName::new("local rollback"),
                        role: AktorNoRole,
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok(())
                        }),
                        closures: AktorClosures {
                            end: Some(
                                (async move |_: ()| {
                                    counted.set(counted.get() + 1);
                                    Err(AktorCleanupError::new("local rollback refused"))
                                })
                                .into(),
                            ),
                            ..AktorClosures::new(async || Ok(()))
                        },
                        options: None,
                    },
                    AktorSetup {
                        name: AktorName::new("local setup failure"),
                        role: AktorNoRole,
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok(())
                        }),
                        closures: AktorClosures::new(async || {
                            Err::<(), _>(AktorSetupError::new("local setup refused"))
                        }),
                        options: None,
                    },
                ),
            )
            .await
            .err()
            .unwrap();

            assert!(error.to_string().contains("local setup refused"));
            assert!(std::error::Error::source(&error).is_some());

            let report = error.report.unwrap();

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

#[cfg(feature = "local")]
#[tokio::test]
async fn cleanup_diagnostic() {
    use super::support::poll;

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let actors = start(AktorSetup {
                name: AktorName::new("cleanup only"),
                role: AktorNoRole,
                kind: AktorKind::TokioLocal(&executor),
                closures: AktorClosures {
                    end: Some(
                        (async |_: ()| Err(AktorCleanupError::new("cleanup refused"))).into(),
                    ),
                    ..AktorClosures::new(async || Ok(()))
                },
                options: None,
            })
            .await
            .unwrap();

            let report = actors.shutdown().await;

            assert!(report.failed());
            assert_eq!(report.actors[0].diagnostics.len(), 1, "{report}");
        })
        .await;

    let mut group = local::AktorGroup::<local::clock::Tokio>::new();
    let mut driver = group.listen().unwrap();
    let _handle = group
        .spawn::<(), 1, ()>(ActorArgs {
            name: "driven cleanup only".into(),
            capacity: 1,
            setup: async || Ok(()),
            cleanup: async |_| Err(AktorCleanupError::new("cleanup refused")),
        })
        .unwrap();

    assert!(poll(&mut driver).is_pending());
    group.killswitch().stop();

    let report = driver.await;

    assert!(report.failed());
    assert_eq!(report.actors[0].diagnostics.len(), 1, "{report}");
}
