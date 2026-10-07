use super::*;

#[tokio::test]
async fn backup() {
    let (saved, backup) = tokio::sync::oneshot::channel();
    let mut saved = Some(saved);
    let mut destination = rusqlite::Connection::open_in_memory().unwrap();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("SQLite backup"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || {
                    let db = rusqlite::Connection::open_in_memory()
                        .map_err(|error| AktorSetupError::new(error.to_string()))?;

                    db.execute_batch("CREATE TABLE saved(value); INSERT INTO saved VALUES(85)")
                        .map_err(|error| AktorSetupError::new(error.to_string()))?;
                    Ok(db)
                },
                intervals: vec![AktorInterval {
                    every: Duration::from_millis(1),
                    run: (async move |db: &mut rusqlite::Connection| {
                        let result = (|| {
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

                end: None,
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
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("Haandboldfuglen"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok::<_, AktorSetupError>(Counter(Rc::new(Cell::new(0)))),
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
            options: AktorNewOptions { capacity: 32 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(200),
        },
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
                let actors = aktor_start(AktorSetup {
                    actors: AktorNew {
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
                        options: AktorNewOptions { capacity: 32 },
                    },
                    shutdown: |_| {},
                    options: AktorOptions {
                        shutdown_grace: Duration::from_millis(200),
                    },
                })
                .await
                .unwrap();

                observed.notified().await;

                let report = actors.shutdown().await;

                assert!(report.timed_out);
                assert!(report.failure.is_none(), "{report}");

                let interval = IntervalDrop;
                let actors = aktor_start(AktorSetup {
                    actors: AktorNew {
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
                        options: AktorNewOptions { capacity: 32 },
                    },
                    shutdown: |_| {},
                    options: AktorOptions {
                        shutdown_grace: Duration::from_secs(5),
                    },
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
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("idle interval"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok::<_, AktorSetupError>(0u32),
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
            options: AktorNewOptions { capacity: 32 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    let report = actors.shutdown().await;

    assert!(report.failed(), "{report}");
    assert_eq!(report.failure.unwrap().actor, "idle interval");
}

async fn close_interval(waiting: bool) {
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let performed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let end = cleaned.clone();
    let actors = aktor_start(AktorSetup {
        actors: aktor_setups! {
            timed: AktorNew {

                name: AktorName::new("interval owner"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
start: async || Ok::<_, AktorSetupError>(0_u32),
                    end: Some((async move |_: u32| {
                        end.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(())
                    }).into()),
                    intervals: vec![AktorInterval {
                        every: Duration::from_secs(1),
                        run: (async |value: &mut u32| { *value += 1; }).into(),
                    }],
                     before_each: None, after_each: None,
                },
                options: AktorNewOptions { capacity: 1 },
            },
            sibling: AktorNew {

                name: AktorName::new("sibling"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
start: async || Ok::<_, AktorSetupError>(0_u32),  end: None, intervals: vec![], before_each: None, after_each: None },
                options: AktorNewOptions { capacity: 32 },
            },
        },
        shutdown: |_| {},
        options: AktorOptions { shutdown_grace: Duration::from_secs(5) },
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
        .send()
        .now_or_never();

        if queued.is_none() {
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
) -> AktorNew<u32, impl core::ops::AsyncFnOnce() -> Result<u32, AktorSetupError> + 'static, Kind>
where
    Kind: setup::kind::AktorMode<
            Each<u32> = dyn FnMut(&mut u32, operation::Operation),
            End<u32> = dyn setup::AktorEnd<u32>,
            Interval<u32> = dyn setup::AktorIntervalLogic<u32>,
        >,
{
    AktorNew {
        name: AktorName::new(if timed { "interval owner" } else { "sibling" }),
        role: AktorNoRole,
        kind,
        closures: AktorClosures {
            start: async || Ok(0_u32),
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

            before_each: None,
            after_each: None,
        },
        options: AktorNewOptions { capacity: 1 },
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
        .send()
        .now_or_never();

        if queued.is_none() {
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
async fn local_close() {
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

                        let actors = aktor_start(AktorSetup {
                            actors: (
                                local_interval(make_kind(), true, cleaned.clone()),
                                local_interval(make_kind(), false, sibling),
                            ),
                            shutdown: |_| {},
                            options: AktorOptions {
                                shutdown_grace: Duration::from_secs(5),
                            },
                        })
                        .await
                        .unwrap();

                        close_local_interval(actors, cleaned, waiting).await;
                    } else {
                        let actors = aktor_start(AktorSetup {
                            actors: (
                                local_interval(
                                    AktorKind::TokioLocal(&executor),
                                    true,
                                    cleaned.clone(),
                                ),
                                local_interval(AktorKind::TokioLocal(&executor), false, sibling),
                            ),
                            shutdown: |_| {},
                            options: AktorOptions {
                                shutdown_grace: Duration::from_secs(5),
                            },
                        })
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
async fn pause() {
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
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("interval counter"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok::<_, AktorSetupError>(0_u32),
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
