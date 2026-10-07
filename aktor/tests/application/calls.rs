use super::*;
use std::sync::atomic::AtomicBool;
use tokio::sync::{Notify, mpsc};

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
            actors: AktorNew {
                name: AktorName::new("application database"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(0_u32),
                    end: Some(
                        (async move |_: u32| {
                            while cleanup_disposed.load(Ordering::SeqCst) != 4 {
                                tokio::task::yield_now().await;
                            }
                            cleanup_done.store(true, Ordering::SeqCst);
                            Err(AktorCleanupError::new("cleanup report"))
                        })
                        .into(),
                    ),

                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 1 },
            },
            shutdown: move |report| {
                assert!(hook_cleaned.load(Ordering::SeqCst));
                closed.send(report).unwrap();
            },
            options: AktorOptions {
                shutdown_grace: Duration::from_secs(2),
            },
        })
        .await
        .unwrap();
        let completion = actors.completion();
        assert_eq!(count(&actors.handles).await, Err("ordinary query error"));
        assert!(!actors.killswitch().is_stopping());
        let entered = Arc::new(Notify::new());
        let (release, released) = oneshot::channel();
        let mut reply = if mode == "cast" {
            crash(&actors.handles, entered.clone(), released)
                .cast()
                .await;
            None
        } else {
            Some(
                crash(&actors.handles, entered.clone(), released)
                    .send()
                    .await,
            )
        };
        entered.notified().await;
        if mode == "drop" {
            drop(reply.take());
        }
        let (ready, mut started) = mpsc::channel(4);
        for index in 0..3 {
            let held = Held(disposed.clone());
            let handle = actors.handles.new_handle();
            let ready = ready.clone();
            let reply = if index == 0 { reply.take() } else { None };
            let after = continued.clone();
            actors
                .group
                .spawn_task(async move {
                    let _held = held;
                    ready.send(()).await.unwrap();
                    if let Some(reply) = reply {
                        reply.await;
                    } else {
                        let _value = count(&handle).await;
                    }
                    after.store(true, Ordering::SeqCst);
                })
                .unwrap();
        }
        let held = Held(disposed.clone());
        let kill = actors.killswitch();
        let handle = actors.handles.new_handle();
        let after = continued.clone();
        let external = tokio::spawn(async move {
            let _held = held;
            ready.send(()).await.unwrap();
            tokio::select! {
                biased;
                _ = kill.wait_stopping() => {},
                _ = count(&handle) => after.store(true, Ordering::SeqCst),
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
        assert_eq!(
            result.failure.as_ref().unwrap().actor,
            "application database"
        );
        assert!(
            result
                .actors
                .iter()
                .flat_map(|actor| &actor.diagnostics)
                .any(|error| error.to_string().contains("cleanup report"))
        );
        assert_eq!(completion.wait().await.to_string(), result.to_string());
    }
}

#[tokio::test]
async fn task_failure() {
    let after = Arc::new(AtomicBool::new(false));
    let continued = after.clone();
    let report = tokio::time::timeout(
        Duration::from_secs(3),
        AktorGroup::new().run(
            async |group| {
                group.spawn_task(async { panic!("dependent task failed") })?;
                core::future::pending::<()>().await;
                continued.store(true, Ordering::SeqCst);
                Ok::<_, AktorError>(())
            },
            async |_| Ok::<_, AktorCleanupError>(()),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();

    assert!(!after.load(Ordering::SeqCst));
    assert_eq!(report.failure.as_ref().unwrap().phase, "task");
    assert!(
        report
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("dependent task failed")
    );
}

#[tokio::test]
async fn callers() {
    for drop_owner in [false, true] {
        let disposed = Arc::new(AtomicUsize::new(0));
        let (closed, closing) = oneshot::channel();
        let actors = aktor_start(AktorSetup {
            actors: AktorNew {
                name: AktorName::new("application cancellation"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(0_u32),
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
            shutdown: async move |report| {
                closed.send(report).unwrap();
                Ok::<_, AktorCleanupError>(())
            },
            options: AktorOptions {
                shutdown_grace: Duration::from_secs(5),
            },
        })
        .await
        .unwrap();
        let completion = actors.completion();
        let (ready, started) = oneshot::channel();
        let hold = Held(disposed.clone());
        actors
            .group
            .spawn_task(async move {
                let _hold = hold;
                ready.send(()).unwrap();
                core::future::pending::<()>().await;
            })
            .unwrap();
        started.await.unwrap();
        if drop_owner {
            drop(actors);
        } else {
            let report = actors.shutdown().await;
            assert!(!report.failed(), "{report}");
        }
        let report = tokio::time::timeout(Duration::from_secs(3), closing)
            .await
            .unwrap()
            .unwrap();
        assert!(!report.failed(), "{report}");
        assert_eq!(disposed.load(Ordering::SeqCst), 1);
        assert_eq!(completion.wait().await.to_string(), report.to_string());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup() {
    use std::sync::{Condvar, Mutex};

    struct WaitingDrop(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for WaitingDrop {
        fn drop(&mut self) {
            let (state, changed) = &*self.0;
            let (done, _) = changed
                .wait_timeout_while(state.lock().unwrap(), Duration::from_secs(2), |done| !*done)
                .unwrap();
            assert!(
                *done,
                "caller disposal waited for cleanup that never started"
            );
        }
    }

    let cleaned = Arc::new((Mutex::new(false), Condvar::new()));
    let cleanup = cleaned.clone();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("coordinated shutdown"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok::<_, AktorSetupError>(0_u32),
                end: Some(
                    (async move |_: u32| {
                        let (done, changed) = &*cleanup;
                        *done.lock().unwrap() = true;
                        changed.notify_all();
                        Ok(())
                    })
                    .into(),
                ),

                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 1 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(3),
        },
    })
    .await
    .unwrap();
    let held = WaitingDrop(cleaned.clone());
    let (ready, started) = oneshot::channel();
    actors
        .group
        .spawn_task(async move {
            let _held = held;
            ready.send(()).unwrap();
            core::future::pending::<()>().await;
        })
        .unwrap();
    started.await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), actors.shutdown())
        .await
        .unwrap();
    assert!(!result.failed(), "{result}");
    assert!(*cleaned.0.lock().unwrap());
}

#[tokio::test]
async fn shutdown_hook_errors() {
    let mut group = AktorGroup::new();
    let (closed, closing) = oneshot::channel();
    group
        .on_shutdown(move |report| {
            closed.send(report).unwrap();
        })
        .unwrap();
    let completion = group.start().unwrap();
    group.killswitch().stop();
    let reported = closing.await.unwrap();
    assert_eq!(completion.await.to_string(), reported.to_string());

    let disposed = Arc::new(AtomicUsize::new(0));
    let called = Arc::new(AtomicUsize::new(0));
    let calls = called.clone();
    let held = Held(disposed.clone());
    let startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("abandoned startup"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok::<_, AktorSetupError>(0_u32),
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        },
        shutdown: async move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            drop(held);
            Ok::<_, AktorCleanupError>(())
        },
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    });
    let completion = startup.completion();
    drop(startup);
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(!completion.wait().await.failed());

    for (panic, actor_failed) in [(true, true), (false, false), (true, false), (false, true)] {
        let called = Arc::new(AtomicUsize::new(0));
        let calls = called.clone();
        let startup = aktor_start(AktorSetup {
            actors: AktorNew {
                name: AktorName::new("shutdown hook"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(0_u32),
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
            shutdown: async move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                if panic {
                    panic!("shutdown hook panicked");
                }
                Err(AktorCleanupError::new("shutdown hook failed"))
            },
            options: AktorOptions {
                shutdown_grace: Duration::from_secs(5),
            },
        });
        let actors = startup.await.unwrap();
        assert!(actors.group.on_shutdown(async |_| Ok(())).is_err());
        if actor_failed {
            message::call(
                &actors.handles.handle,
                |_: &mut u32, ()| panic!("initial actor failure"),
                (),
            )
            .cast()
            .await;
            tokio::time::timeout(Duration::from_secs(3), actors.killswitch().wait_stopping())
                .await
                .unwrap();
        }

        let report = tokio::time::timeout(Duration::from_secs(3), actors.shutdown())
            .await
            .unwrap();
        assert_eq!(called.load(Ordering::SeqCst), 1);
        assert!(report.failed());
        if actor_failed {
            assert!(
                report
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("initial actor failure")
            );
        }
        let message = if panic {
            "shutdown hook panicked"
        } else {
            "shutdown hook failed"
        };

        assert!(
            report
                .application
                .iter()
                .any(|error| error.to_string().contains(message)),
            "{report}"
        );
        assert!(report.to_string().contains(message));
    }
}
