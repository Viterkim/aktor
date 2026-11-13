use super::*;

#[cfg(feature = "std_thread")]
#[test]
fn mixed_group_runtime() {
    use aktor::{AktorKind, AktorNew, AktorSetup, AktorStuck, aktor_ready, aktor_start};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let actors = runtime
        .block_on(aktor_start(AktorSetup {
            actors: (
                AktorNew {
                    name: "standard".into(),
                    kind: AktorKind::StdThread,
                    start: async || aktor_ready(()),
                    end: (),
                    hooks: Default::default(),
                    capacity: 1,
                    stuck: AktorStuck::Kill,
                },
                AktorNew {
                    name: "tokio".into(),
                    kind: AktorKind::TokioThread,
                    start: async || aktor_ready(()),
                    end: (),
                    hooks: Default::default(),
                    capacity: 1,
                    stuck: AktorStuck::Kill,
                },
            ),
            shutdown: |_| {},
            grace: Duration::from_secs(1),
        }))
        .unwrap();

    drop(runtime);

    assert!(actors.group.spawner(false).is_ok());
}

#[tokio::test]
async fn startup_failure() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let closed = cleaned.clone();
    let mut group = AktorGroup::new();
    let completed = group.start().unwrap();

    group
        .spawn_args(AktorArgs {
            name: "opened resource".into(),
            capacity: 1,
            setup: || Ok::<_, String>(()),
            cleanup: move |_| {
                closed.store(true, Ordering::SeqCst);
                Ok::<_, String>(())
            },
        })
        .await
        .unwrap();

    let failed = group
        .spawn_args(AktorArgs {
            name: "failed setup".into(),
            capacity: 1,
            setup: || Err::<(), _>(String::from("setup went wrong")),
            cleanup: |_| Ok::<_, String>(()),
        })
        .await;

    assert!(failed.is_err());

    let report = completed.wait().await;

    assert!(report.startup);
    assert!(cleaned.load(Ordering::SeqCst));
    assert_eq!(report.failure.as_ref().unwrap().phase, "setup");
    assert_eq!(report.to_string().matches("setup went wrong").count(), 1);
}

struct ReleaseOnDrop(Arc<Notify>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn cancelled_startup() {
    for setup_fails in [true, false] {
        let mut group = AktorGroup::with_grace(Duration::from_secs(1));
        let kill = group.killswitch();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let inside = entered.clone();
        let setup_release = release.clone();
        let trigger = tokio::spawn(async move {
            entered.notified().await;
            kill.stop();
        });

        let completed = group.start().unwrap();
        let killswitch = group.killswitch();

        killswitch
            .run(async move {
                let _release = ReleaseOnDrop(release);
                group
                    .spawn_async(AktorArgs {
                        name: "cancelled startup".into(),
                        capacity: 1,
                        setup: async move || {
                            inside.notify_one();
                            setup_release.notified().await;

                            if setup_fails {
                                Err(String::from("late setup error"))
                            } else {
                                Ok(())
                            }
                        },
                        cleanup: async |_| Err(String::from("late cleanup error")),
                    })
                    .await
                    .unwrap();
            })
            .await;
        killswitch.stop();

        let result = completed.wait().await;

        trigger.await.unwrap();

        assert!(!result.ran_past_grace);

        let expected = if setup_fails {
            "late setup error"
        } else {
            "late cleanup error"
        };

        let failure = result.failure.as_ref().unwrap();

        assert_eq!(failure.phase, if setup_fails { "setup" } else { "cleanup" });
        assert_eq!(result.to_string().matches(expected).count(), 1);

        assert_eq!(failure.message, expected);
    }
}

#[tokio::test]
async fn startup() {
    for fail in [false, true] {
        let mut actors = AktorGroup::new();
        let kill = actors.killswitch();
        let completed = actors.completion();
        let hooks = Arc::new(AtomicUsize::new(0));
        let hook_count = hooks.clone();
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleanup = cleaned.clone();
        let closing = actors
            .start_with(async move |_| {
                hook_count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(())
            })
            .unwrap();

        assert!(actors.start_with(async |_| Ok::<_, String>(())).is_err());

        let setup = || Ok::<_, String>(0usize);
        let cleanup = move |_| {
            cleanup.store(true, Ordering::SeqCst);
            Ok::<_, String>(())
        };
        let database = actors
            .spawn_args(AktorArgs::new("database", setup, cleanup))
            .await
            .unwrap();
        let caller_kill = kill.clone();
        let started = Arc::new(Notify::new());
        let caller_started = started.clone();
        let caller = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = caller_kill.wait_stopping() => {},
                _ = async {
                    let result = call(&database.handle, |_, ()| Err::<(), _>("query failed"), ()).await;
                    assert_eq!(result, Err("query failed"));
                    assert!(!caller_kill.is_stopping());
                    if fail {
                        caller_started.notify_one();
                        call(&database.handle, |_, ()| panic!("storage died"), ()).await;
                    } else {
                        caller_started.notify_one();
                        core::future::pending::<()>().await;
                    }
                } => panic!("caller continued after permanent loss"),
            }
        });

        started.notified().await;

        if !fail {
            kill.stop();
        }

        let report = tokio::time::timeout(Duration::from_secs(2), closing.wait())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), caller)
            .await
            .unwrap()
            .unwrap();
        assert!(cleaned.load(Ordering::SeqCst));
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        assert_eq!(report.failed(), fail);
        assert_eq!(completed.wait().await.to_string(), report.to_string());

        if fail {
            assert!(
                report
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("storage died")
            );
        }

        assert!(matches!(
            actors
                .spawn_args(AktorArgs::new(
                    "late",
                    || Ok::<_, String>(()),
                    |_| Ok::<_, String>(())
                ))
                .await,
            Err((
                aktor::owner::StartError {
                    kind: AktorStartKind::Stopping,
                    ..
                },
                _
            ))
        ));
    }
}

#[tokio::test]
async fn default_group() {
    let mut actors = AktorGroup::new();
    let closing = actors.start().unwrap();
    let actor = actors.spawn_value("counter", 17usize).await.unwrap();
    let shutdown = actors.shutdown();

    assert!(actors.killswitch().is_stopping());
    assert!(!(&closing).await.failed());
    assert_eq!(closing.await.to_string(), shutdown.await.to_string());
    assert!(actor.completion().wait().await.is_ok());
}

#[tokio::test]
async fn startup_rollback() {
    let mut actors = AktorGroup::new();
    let hooks = Arc::new(AtomicBool::new(false));
    let hook = hooks.clone();
    let closing = actors
        .start_with(async move |_| {
            hook.store(true, Ordering::SeqCst);
            Ok::<_, String>(())
        })
        .unwrap();

    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup = cleaned.clone();
    let _sibling = actors
        .spawn_args(AktorArgs::new(
            "audio",
            || Ok::<_, String>(()),
            move |_| {
                cleanup.store(true, Ordering::SeqCst);
                Err(String::from("audio cleanup failed"))
            },
        ))
        .await
        .unwrap();

    let startup = tokio::spawn(async move {
        actors
            .spawn_args(AktorArgs::new(
                "storage",
                || Err::<(), _>(String::from("could not open")),
                |_| Ok::<_, String>(()),
            ))
            .await?;
        Ok::<_, (aktor::owner::StartError, String)>(())
    });

    let (failed, diagnostics) = tokio::time::timeout(Duration::from_secs(2), startup)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();

    let aktor::listener::DedicatedStartError::Init(error) = failed.cause.primary() else {
        panic!("typed setup error was lost");
    };

    assert_eq!(error.diagnostics, "could not open");

    for message in ["could not open", "audio cleanup failed"] {
        assert_eq!(diagnostics.matches(message).count(), 1);
    }

    assert!(cleaned.load(Ordering::SeqCst));
    assert!(hooks.load(Ordering::SeqCst));

    let report = closing.wait().await;

    assert_eq!(report.failure.unwrap().actor, "storage");
}

#[tokio::test]
async fn listener_cancel() {
    for setup_fails in [false, true] {
        let mut actors = AktorGroup::new();
        let kill = actors.killswitch();
        let closing = actors.start_with(async |_| Ok::<_, String>(())).unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let setup_entered = entered.clone();
        let setup_release = release.clone();
        let startup = tokio::spawn(async move {
            actors
                .spawn_async(AktorArgs::new(
                    "storage",
                    move || async move {
                        setup_entered.notify_one();
                        setup_release.notified().await;

                        if setup_fails {
                            Err(String::from("late setup failed"))
                        } else {
                            Ok(())
                        }
                    },
                    async |_| Err::<(), _>(String::from("late cleanup failed")),
                ))
                .await
        });

        entered.notified().await;
        kill.stop();
        startup.abort();
        assert!(startup.await.err().unwrap().is_cancelled());
        release.notify_one();

        let report = tokio::time::timeout(Duration::from_secs(2), closing.wait())
            .await
            .unwrap();
        let expected = if setup_fails {
            "late setup failed"
        } else {
            "late cleanup failed"
        };

        assert!(report.failed());

        let failure = report.failure.as_ref().unwrap();
        assert_eq!(failure.phase, if setup_fails { "setup" } else { "cleanup" });
        assert_eq!(report.to_string().matches(expected).count(), 1);
        assert_eq!(failure.message, expected);
    }
}
