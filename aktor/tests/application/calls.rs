use super::*;
use core::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};

#[aktor]
async fn count(state: &u32) -> Result<u32, &'static str> {
    if *state == 0 {
        Err("ordinary query error")
    } else {
        Ok(*state)
    }
}

#[aktor]
async fn crash(_state: &mut u32, entered: Arc<Notify>, release: oneshot::Receiver<()>) {
    entered.notify_one();
    release.await.unwrap();
    panic!("owner failed");
}

struct Held(Arc<AtomicUsize>);
impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn shutdown() {
    for mode in ["reply", "drop", "cast"] {
        let disposed = Arc::new(AtomicUsize::new(0));
        let cleaned = Arc::new(AtomicBool::new(false));
        let continued = Arc::new(AtomicBool::new(false));
        let cleanup_disposed = disposed.clone();
        let cleanup_done = cleaned.clone();
        let (closed, closing) = oneshot::channel();
        let hook_cleaned = cleaned.clone();
        let actors = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "application database".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: async move |_: u32| {
                    while cleanup_disposed.load(Ordering::SeqCst) != 4 {
                        tokio::task::yield_now().await;
                    }
                    cleanup_done.store(true, Ordering::SeqCst);
                    Err(((), String::from("cleanup report")))
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::MIN,
                stuck: AktorStuck::Kill,
            },
            shutdown: move |report| {
                assert!(hook_cleaned.load(Ordering::SeqCst));
                closed.send(report).unwrap();
            },
            grace: Duration::from_secs(2),
        });
        actors.ready().await.unwrap();
        assert_eq!(
            count(actors.handles()).await,
            Ok(Err("ordinary query error"))
        );
        assert!(!actors.killswitch().is_stopping());
        let entered = Arc::new(Notify::new());
        let (release, released) = oneshot::channel();
        let mut reply = if mode == "cast" {
            crash(actors.handles(), entered.clone(), released)
                .cast()
                .await
                .unwrap();
            None
        } else {
            Some(
                crash(actors.handles(), entered.clone(), released)
                    .send()
                    .await
                    .unwrap(),
            )
        };
        entered.notified().await;
        if mode == "drop" {
            drop(reply.take());
        }
        let (ready, mut started) = mpsc::channel(4);
        for index in 0..3 {
            let held = Held(disposed.clone());
            let handle = actors.handles().clone();
            let ready = ready.clone();
            let reply = if index == 0 { reply.take() } else { None };
            let after = continued.clone();
            actors
                .group()
                .spawn_task(
                    async move {
                        let _held = held;
                        ready.send(()).await.unwrap();
                        if let Some(reply) = reply {
                            if reply.await.is_err() {
                                return;
                            }
                        } else {
                            if count(&handle).await.is_err() {
                                return;
                            }
                        }
                        after.store(true, Ordering::SeqCst);
                    },
                    aktor::AktorJob::DropAtStop,
                )
                .unwrap();
        }
        let held = Held(disposed.clone());
        let kill = actors.killswitch();
        let handle = actors.handles().clone();
        let after = continued.clone();
        let external = tokio::spawn(async move {
            let _held = held;
            ready.send(()).await.unwrap();
            tokio::select! {
                biased;
                _ = kill.wait_stopping() => {},
                Ok(_) = count(&handle) => after.store(true, Ordering::SeqCst),
            }
        });
        for _ in 0..4 {
            started.recv().await.unwrap();
        }
        release.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(4), closing)
            .await
            .unwrap()
            .unwrap();
        external.await.unwrap();
        assert!(!continued.load(Ordering::SeqCst));
        assert_eq!(disposed.load(Ordering::SeqCst), 4);
        assert!(
            result
                .failures
                .iter()
                .any(|failure| failure.error.contains("cleanup report"))
        );
    }
}

#[tokio::test]
async fn task_failure() {
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: support::thread_actor((), 1),
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let completed = started.completion();
    let group = started.group();

    group
        .spawn_task::<(), _>(
            async { panic!("dependent task failed") },
            aktor::AktorJob::DropAtStop,
        )
        .unwrap();

    let report = tokio::time::timeout(Duration::from_secs(3), completed.wait())
        .await
        .unwrap()
        .unwrap();
    let failure = &report.failures[0];

    assert_eq!(failure.source, aktor::AktorFailureSource::ApplicationTask);
    assert!(failure.error.contains("dependent task failed"));
}

#[tokio::test]
async fn callers() {
    for drop_owner in [false, true] {
        let disposed = Arc::new(AtomicUsize::new(0));
        let (closed, closing) = oneshot::channel();
        let actors = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "application cancellation".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: async move |report| {
                closed.send(report).unwrap();
            },
            grace: Duration::from_secs(5),
        });
        actors.ready().await.unwrap();
        let (ready, started) = oneshot::channel();
        let hold = Held(disposed.clone());
        actors
            .group()
            .spawn_task(
                async move {
                    let _hold = hold;
                    ready.send(()).unwrap();
                    core::future::pending::<()>().await;
                },
                aktor::AktorJob::DropAtStop,
            )
            .unwrap();
        started.await.unwrap();
        if drop_owner {
            drop(actors);
        } else {
            let _ = actors.stop().await;
        }
        let report = tokio::time::timeout(Duration::from_secs(3), closing)
            .await
            .unwrap()
            .unwrap();
        assert!(!report.failed(), "{report}");
        assert_eq!(disposed.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn group_task_order() {
    let (after, after_closed) = oneshot::channel();
    let (finished, mut done) = oneshot::channel();
    let actor = |end| AktorNew {
        name: "storage".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || ()),
        end,
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    };

    let started = aktor_start(AktorSetup {
        actors: actor(async move |()| {
            done.try_recv().unwrap();
        }),
        shutdown: |_| {},
        grace: Duration::from_secs(2),
        stuck: AktorGroupStuck::KeepRunning,
    });
    started.ready().await.unwrap();
    let group = started.group();
    group
        .spawn_task(
            async move {
                after_closed.await.unwrap();
                finished.send(()).unwrap();
            },
            AktorJob::FinishBeforeEnd,
        )
        .unwrap();

    let _later = group
        .spawn(AktorNew {
            name: "listener".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || ()),
            end: async move |()| {
                after.send(()).unwrap();
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        })
        .ready()
        .await
        .unwrap();

    assert!(!started.stop().await.failed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_task_drop() {
    struct WaitForCleanup(std::sync::mpsc::Receiver<()>);
    impl Drop for WaitForCleanup {
        fn drop(&mut self) {
            self.0.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    for release_in_hook in [false, true] {
        let (release, waiting) = std::sync::mpsc::channel();
        let (end, hook) = if release_in_hook {
            (None, Some(release))
        } else {
            (Some(release), None)
        };
        let started = aktor_start(AktorSetup {
            actors: AktorNew {
                name: "storage".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Ready(async || ()),
                end: async move |()| {
                    if let Some(release) = end {
                        release.send(()).unwrap();
                    }
                },
                capacity: NonZeroUsize::MIN,
                hooks: AktorHooks::NONE,
                stuck: AktorStuck::Kill,
            },
            shutdown: move |_| {
                if let Some(release) = hook {
                    release.send(()).unwrap();
                }
            },
            grace: Duration::from_millis(100),
            stuck: AktorGroupStuck::KeepRunning,
        });
        started.ready().await.unwrap();
        let hold = WaitForCleanup(waiting);
        started
            .group()
            .spawn_task(
                async move {
                    let _held = hold;
                    core::future::pending::<()>().await;
                },
                AktorJob::DropAtStop,
            )
            .unwrap();

        let retired = started.stop_released().await.unwrap();
        assert!(
            retired
                .report()
                .late_work
                .contains(&AktorLateWork::Application)
        );
        assert!(!retired.report().failed());
    }
}

#[tokio::test]
async fn shutdown_hook_errors() {
    for (panic, actor_failed) in [(true, true), (false, false), (true, false), (false, true)] {
        let called = Arc::new(AtomicUsize::new(0));
        let calls = called.clone();
        let actors = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "shutdown hook".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: async move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                if panic {
                    panic!("shutdown hook panicked");
                }
                Err(((), String::from("shutdown hook failed")))
            },
            grace: Duration::from_secs(5),
        });

        actors.ready().await.unwrap();

        if actor_failed {
            fail_initial(actors.handles()).cast().await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), actors.killswitch().wait_stopping())
                .await
                .unwrap();
        }

        let report = tokio::time::timeout(Duration::from_secs(3), actors.stop())
            .await
            .unwrap();
        assert_eq!(called.load(Ordering::SeqCst), 1);
        if actor_failed {
            assert!(report.failures[0].error.contains("initial actor failure"));
        }
        let message = if panic {
            "shutdown hook panicked"
        } else {
            "shutdown hook failed"
        };

        let text = report.to_string();
        assert_eq!(text.matches(message).count(), 1, "{text}");
        if actor_failed || !panic {
            assert!(
                report
                    .failures
                    .iter()
                    .any(|failure| failure.error.contains(message)),
                "{text}"
            );
        }
    }
}

#[aktor]
async fn fail_initial(_: &u32) {
    panic!("initial actor failure");
}

#[tokio::test]
async fn group_task_error() {
    let group = aktor_start(AktorSetup {
        actors: support::thread_actor((), 1),
        shutdown: |_| {},
        stuck: AktorGroupStuck::KeepRunning,
        grace: Duration::from_secs(1),
    });
    group.ready().await.unwrap();
    group
        .group()
        .spawn_task(
            async { Err::<(), _>(((), String::from("tenant error"))) },
            AktorJob::FinishBeforeEnd,
        )
        .unwrap();

    let report = group.completion().wait().await.unwrap();
    let [failure] = report.failures.as_slice() else {
        panic!("expected one task failure")
    };
    assert_eq!(report.stop_cause, StopCause::Failure(failure.id));
    assert_eq!(failure.source, AktorFailureSource::ApplicationTask);
}
