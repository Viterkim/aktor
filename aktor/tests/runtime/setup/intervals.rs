use super::*;
use core::num::NonZeroUsize;
use er::*;
use std::time::Duration;

#[derive(Er)]
enum BackupErr {
    Open,
    Schema,
}

#[tokio::test]
async fn backup() {
    let (saved, backup) = tokio::sync::oneshot::channel();
    let mut saved = Some(saved);
    let mut destination = rusqlite::Connection::open_in_memory().unwrap();
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "SQLite backup".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Try(async || {
                let db = rusqlite::Connection::open_in_memory().er(|| BackupErr::Open)?;

                db.execute_batch("CREATE TABLE saved(value); INSERT INTO saved VALUES(85)")
                    .er(|| BackupErr::Schema)?;
                Ok::<_, ErTree<BackupErr>>(db)
            }),
            end: (),
            hooks: AktorHooks {
                intervals: vec![
                    AktorInterval::new(
                        Duration::from_millis(1),
                        async move |db: &mut rusqlite::Connection| {
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
                        },
                    )
                    .unwrap(),
                ],
                before_each: (),
                after_each: (),
            },
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    assert_eq!(backup.await.unwrap().unwrap(), 85);
    assert!(!actors.stop().await.failed());

    struct IntervalDrop;
    impl Drop for IntervalDrop {
        fn drop(&mut self) {
            panic!("interval drop died");
        }
    }

    let observed = Arc::new(Notify::new());
    let notify = observed.clone();
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "Haandboldfuglen".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || Counter(Rc::new(Cell::new(0)))),
            end: (),
            hooks: AktorHooks {
                intervals: vec![
                    AktorInterval::new(
                        Duration::from_millis(1),
                        async move |counter: &mut Counter| {
                            counter.0.set(85);
                            notify.notify_one();
                            std::future::pending::<()>().await;
                        },
                    )
                    .unwrap(),
                ],
                before_each: (),
                after_each: (),
            },
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_millis(200),
    });
    actors.ready().await.unwrap();

    observed.notified().await;

    let report = actors.stop_released().await.unwrap().into_report();

    assert!(report.ran_past_grace());
    assert!(report.failed(), "{report}");

    #[cfg(feature = "local")]
    {
        let executor = tokio::task::LocalSet::new();

        executor
            .run_until(async {
                let observed = Arc::new(Notify::new());
                let notify = observed.clone();
                let actors = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::KeepRunning,
                    actors: AktorNew {
                        name: "local interval".into(),
                        kind: AktorKind::TokioLocal(&executor),
                        start: aktor::AktorInit::Ready(async || 0_u32),
                        end: (),
                        hooks: AktorHooks {
                            intervals: vec![AktorInterval::new(Duration::from_millis(1), async move |_: &mut u32| {
                                    notify.notify_one();
                                    std::future::pending::<()>().await;
                                }).unwrap()],
                            before_each: (),
                            after_each: (),
                        },
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_millis(200),
                });
                actors.ready().await.unwrap();

                observed.notified().await;

                let report = actors.stop().await;

                assert!(report.ran_past_grace());
                assert_eq!(report.stop_cause, aktor::StopCause::Requested);

                let interval = IntervalDrop;
                let actors = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::KeepRunning,
                    actors: AktorNew {
                        name: "local idle interval".into(),
                        kind: AktorKind::TokioLocal(&executor),
                        start: aktor::AktorInit::Ready(async || Counter(Rc::new(Cell::new(0)))),
                        end: (),
                        hooks: AktorHooks {
                            intervals: vec![AktorInterval::new(Duration::from_secs(3600), async move |_: &mut Counter| {
                                    let _keep = &interval;
                                }).unwrap()],
                            before_each: (),
                            after_each: (),
                        },
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                });
                actors.ready().await.unwrap();

                let (_input, mut output) = add::latest(actors.handles());
                let report = actors.stop().await;

                assert!(report.failed(), "{report}");
                assert!(matches!(&report.failures[0].source, aktor::AktorFailureSource::Actor { name, .. } if name == "local idle interval"));
                assert!(output.next().await.unwrap().is_err());
                assert!(output.next().await.is_none());
            })
            .await;
    }

    let interval = IntervalDrop;
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "idle interval".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || 0u32),
            end: (),
            hooks: AktorHooks {
                intervals: vec![
                    AktorInterval::new(Duration::from_secs(3600), async move |_: &mut u32| {
                        let _keep = &interval;
                    })
                    .unwrap(),
                ],
                before_each: (),
                after_each: (),
            },
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let report = actors.stop().await;

    assert!(report.failed(), "{report}");
    assert!(
        matches!(&report.failures[0].source, aktor::AktorFailureSource::Actor { name, .. } if name == "idle interval")
    );
}

#[aktor]
async fn count_call(
    _: &mut u32,
    gate: Option<(Arc<Notify>, Arc<Notify>)>,
    count: Arc<std::sync::atomic::AtomicUsize>,
) {
    if let Some((entered, release)) = gate {
        entered.notify_one();
        release.notified().await;
    }
    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

async fn close_interval(waiting: bool) {
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let performed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let end = cleaned.clone();
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: (
            AktorNew {
                name: "interval owner".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: async move |_: u32| {
                    end.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                },
                hooks: AktorHooks {
                    intervals: vec![
                        AktorInterval::new(Duration::from_secs(1), async |value: &mut u32| {
                            *value += 1;
                        })
                        .unwrap(),
                    ],
                    before_each: (),
                    after_each: (),
                },
                capacity: NonZeroUsize::MIN,
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "sibling".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

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
            count_call(&actors.handles().0, Some((signal, release)), count)
                .send()
                .await
                .unwrap(),
        );

        entered.notified().await;

        let count = performed.clone();
        let queued = count_call(&actors.handles().0, None, count)
            .send()
            .now_or_never();

        if queued.is_none() {
            gate.notify_one();
        }

        drop(queued.unwrap());
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
    }

    let completed = actors.handles().0.shutdown();

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
    assert_eq!(user_count(&actors.handles().1).await, Ok(0));
    assert!(!actors.stop().await.failed());
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
#[allow(clippy::type_complexity)]
fn local_interval<Kind, Signature>(
    kind: Kind,
    timed: bool,
    cleaned: Arc<std::sync::atomic::AtomicUsize>,
) -> AktorNew<
    u32,
    aktor::AktorInit<
        impl core::ops::AsyncFnOnce() -> Result<u32, (core::convert::Infallible, String)> + 'static,
    >,
    Kind,
    impl core::ops::AsyncFnMut(u32) + 'static,
    Signature,
>
where
    Kind: setup::kind::AktorMode<
            Each<u32> = dyn FnMut(&mut u32, operation::Operation),
            Interval<u32> = dyn setup::AktorIntervalLogic<u32>,
        >,
{
    AktorNew {
        name: if timed { "interval owner" } else { "sibling" }.into(),
        kind,
        start: aktor::AktorInit::Ready(async || 0_u32),
        end: async move |_: u32| {
            cleaned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        },
        hooks: AktorHooks {
            intervals: if timed {
                vec![
                    AktorInterval::new(Duration::from_secs(1), async |value: &mut u32| {
                        *value += 1;
                    })
                    .unwrap(),
                ]
            } else {
                vec![]
            },
            before_each: (),
            after_each: (),
        },
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
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
type LocalCounter<Clock> = local::Handle<u32, (), Clock>;

#[cfg(feature = "local")]
async fn close_local_interval<Group: setup::group::AktorSetupGroup, Clock, E>(
    actors: AktorStarted<(LocalCounter<Clock>, LocalCounter<Clock>), Group, E>,
    cleaned: Arc<std::sync::atomic::AtomicUsize>,
    waiting: bool,
    stopping: impl Fn(&Group::AktorKillSwitch) -> bool,
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
                &actors.handles().0,
                operation,
                async move |_: &mut u32, ()| {
                    signal.notify_one();
                    release.notified().await;
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                },
                (),
            )
            .send()
            .await
            .unwrap(),
        );

        entered.notified().await;

        let count = performed.clone();
        let queued = local::Request::new(
            &actors.handles().0,
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

    let completed = actors.handles().0.shutdown();

    gate.notify_one();
    completed.await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    awake.abort();
    assert!(!stopping(&actors.killswitch()));
    assert_eq!(
        local::Request::new(
            &actors.handles().1,
            operation,
            async |value: &mut u32, ()| *value,
            (),
        )
        .await,
        Ok(0)
    );

    let probe = local::Request::new(
        &actors.handles().0,
        operation,
        async |value: &mut u32, ()| *value,
        (),
    );

    assert_eq!(
        probe.await.unwrap_err().outcome,
        aktor::AktorDeliveryOutcome::NotAdmitted
    );
    assert!(!stopping(&actors.killswitch()));

    Group::stop(&actors.killswitch());
    assert!(!actors.completion().await.unwrap().failed());
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

                            AktorKind::Local::<IntervalClock, _, _>(move |future| {
                                executor.spawn_local(future);
                                Ok::<(), ((), String)>(())
                            })
                        };

                        let actors = aktor_start(AktorSetup {
                            stuck: aktor::AktorGroupStuck::KeepRunning,
                            actors: (
                                local_interval(make_kind(), true, cleaned.clone()),
                                local_interval(make_kind(), false, sibling),
                            ),
                            shutdown: |_| {},
                            grace: Duration::from_secs(5),
                        });
                        actors.ready().await.unwrap();

                        close_local_interval(actors, cleaned, waiting, |kill| kill.is_stopping())
                            .await;
                    } else {
                        let actors = aktor_start(AktorSetup {
                            stuck: aktor::AktorGroupStuck::KeepRunning,
                            actors: (
                                local_interval(
                                    AktorKind::TokioLocal(&executor),
                                    true,
                                    cleaned.clone(),
                                ),
                                local_interval(AktorKind::TokioLocal(&executor), false, sibling),
                            ),
                            shutdown: |_| {},
                            grace: Duration::from_secs(5),
                        });
                        actors.ready().await.unwrap();

                        close_local_interval(actors, cleaned, waiting, |kill| kill.is_stopping())
                            .await;
                    }
                }
            }
        })
        .await;
}

#[test]
fn zero_interval() {
    let interval =
        AktorInterval::<u32, AktorKind::TokioThread>::new(Duration::ZERO, async |_: &mut u32| {});
    assert!(interval.is_err());
}
