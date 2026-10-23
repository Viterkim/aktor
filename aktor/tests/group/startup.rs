use super::*;

#[tokio::test]
async fn startup_failure() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let closed = cleaned.clone();
    let result = AktorGroup::new()
        .run(
            async |app| -> Result<(), AktorError> {
                let _first = app
                    .spawn(AktorArgs {
                        name: "opened resource".into(),
                        capacity: 1,
                        setup: || Ok::<_, AktorError>(()),
                        cleanup: move |_| {
                            closed.store(true, Ordering::SeqCst);
                            Ok::<_, AktorError>(())
                        },
                    })
                    .await
                    .map_err(|error| AktorError {
                        diagnostics: error.to_string(),
                        data: (),
                    })?;

                let _second = app
                    .spawn(AktorArgs {
                        name: "failed setup".into(),
                        capacity: 1,
                        setup: || {
                            Err::<(), _>(AktorError {
                                diagnostics: "setup went wrong".into(),
                                data: (),
                            })
                        },
                        cleanup: |_| Ok::<_, AktorError>(()),
                    })
                    .await
                    .map_err(|error| AktorError {
                        diagnostics: error.to_string(),
                        data: (),
                    })?;

                Ok(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .unwrap_err();

    assert!(result.report.startup);
    assert!(cleaned.load(Ordering::SeqCst));
    let failure = result.report.failure.as_ref().unwrap();

    assert_eq!(failure.phase, "setup");
    assert_eq!(
        result
            .diagnostics
            .report
            .matches("setup went wrong")
            .count(),
        1
    );
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
        let group = AktorGroup::with_grace(Duration::from_secs(1));
        let kill = group.killswitch();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let inside = entered.clone();
        let setup_release = release.clone();
        let trigger = tokio::spawn(async move {
            entered.notified().await;
            kill.stop();
        });

        let result = group
            .run(
                async move |app| -> Result<(), AktorError> {
                    let _release = ReleaseOnDrop(release);
                    let _owner = app
                        .spawn_async(AktorArgs {
                            name: "cancelled startup".into(),
                            capacity: 1,
                            setup: async move || {
                                inside.notify_one();
                                setup_release.notified().await;

                                if setup_fails {
                                    Err(AktorError::new("late setup error", ()))
                                } else {
                                    Ok(())
                                }
                            },
                            cleanup: async |_| Err(AktorError::new("late cleanup error", ())),
                        })
                        .await
                        .unwrap();

                    Ok(())
                },
                async |_| Ok::<_, AktorError>(()),
            )
            .await
            .unwrap_err();

        trigger.await.unwrap();

        assert!(!result.report.timed_out);

        let expected = if setup_fails {
            "late setup error"
        } else {
            "late cleanup error"
        };

        let failure = result.report.failure.as_ref().unwrap();

        assert_eq!(failure.phase, if setup_fails { "setup" } else { "cleanup" });
        assert_eq!(result.diagnostics.report.matches(expected).count(), 1);

        if setup_fails {
            assert!(result.report.actors[0].diagnostics.is_empty());
            assert!(failure.message.contains(expected));
        } else {
            assert!(
                result.report.actors[0]
                    .diagnostics
                    .iter()
                    .any(|error| error.diagnostics.contains(expected))
            );
        }
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
                Ok::<_, AktorError>(())
            })
            .unwrap();

        assert!(
            actors
                .start_with(async |_| Ok::<_, AktorError>(()))
                .is_err()
        );

        let setup = || Ok::<_, AktorError>(0usize);
        let cleanup = move |_| {
            cleanup.store(true, Ordering::SeqCst);
            Ok::<_, AktorError>(())
        };
        let database = actors
            .spawn(AktorArgs::new("database", setup, cleanup))
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
                        database.actor.pause().await.unwrap();
                        caller_started.notify_one();
                        call(&database.handle, |_, ()| (), ()).await;
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
                .spawn(AktorArgs::new(
                    "late",
                    || Ok::<_, AktorError>(()),
                    |_| Ok::<_, AktorError>(())
                ))
                .await,
            Err(aktor::owner::StartError {
                error: aktor::listener::DedicatedStartError::Closed,
                ..
            })
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

    let count = std::rc::Rc::new(std::cell::Cell::new(0));

    AktorGroup::new()
        .run(
            async |_| Ok::<_, AktorError>(()),
            |_| count.set(count.get() + 1),
        )
        .await
        .unwrap();

    assert_eq!(count.get(), 1);
}

#[tokio::test]
async fn application_creation() {
    use futures_util::FutureExt;
    use std::{future::Ready, panic::AssertUnwindSafe};

    fn application(_: &mut AktorGroup) -> Ready<Result<(), AktorError>> {
        panic!("application creation failed");
    }

    let group = AktorGroup::new();
    let completed = group.completion();
    let cleanup = Arc::new(AtomicBool::new(false));
    let cleaned = cleanup.clone();
    let result = AssertUnwindSafe(group.run(application, async move |_| {
        cleaned.store(true, Ordering::SeqCst);
        Ok::<_, AktorError>(())
    }))
    .catch_unwind()
    .await;

    let report = completed.wait().await;

    assert!(result.is_ok(), "application creation escaped run()");
    assert_eq!(
        report.failure.unwrap().message,
        "application creation failed"
    );
    assert!(cleanup.load(Ordering::SeqCst));
}

#[tokio::test]
async fn startup_rollback() {
    let mut actors = AktorGroup::new();
    let hooks = Arc::new(AtomicBool::new(false));
    let hook = hooks.clone();
    let closing = actors
        .start_with(async move |_| {
            hook.store(true, Ordering::SeqCst);
            Ok::<_, AktorError>(())
        })
        .unwrap();

    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup = cleaned.clone();
    let _sibling = actors
        .spawn(AktorArgs::new(
            "audio",
            || Ok::<_, AktorError>(()),
            move |_| {
                cleanup.store(true, Ordering::SeqCst);
                Err(AktorError::new("audio cleanup failed", ()))
            },
        ))
        .await
        .unwrap();

    let startup = tokio::spawn(async move {
        actors
            .spawn(AktorArgs::new(
                "storage",
                || {
                    Err::<(), _>(AktorError {
                        diagnostics: "could not open".into(),
                        data: (),
                    })
                },
                |_| Ok::<_, AktorError>(()),
            ))
            .await?;
        Ok::<_, aktor::owner::StartError>(())
    });

    let failed = tokio::time::timeout(Duration::from_secs(2), startup)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();

    let aktor::listener::DedicatedStartError::Init(error) = &failed.error else {
        panic!("typed setup error was lost");
    };

    assert_eq!(error.diagnostics, "could not open");

    let tree = ErTree::from(failed);
    let diagnostic = tree.er_find::<aktor::AktorDiagnostic>().unwrap();

    for message in ["could not open", "audio cleanup failed"] {
        assert_eq!(diagnostic.report.matches(message).count(), 1);
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
        let closing = actors
            .start_with(async |_| Ok::<_, AktorError>(()))
            .unwrap();
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
                            Err(AktorError::new("late setup failed", ()))
                        } else {
                            Ok(())
                        }
                    },
                    async |_| Err::<(), _>(AktorError::new("late cleanup failed", ())),
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
        let actor = report
            .actors
            .iter()
            .find(|actor| actor.actor == "storage")
            .unwrap();

        assert_eq!(failure.phase, if setup_fails { "setup" } else { "cleanup" });
        assert_eq!(report.to_string().matches(expected).count(), 1);

        if setup_fails {
            assert!(actor.diagnostics.is_empty());
            assert!(failure.message.contains(expected));
        } else {
            assert!(
                actor
                    .diagnostics
                    .iter()
                    .any(|error| error.diagnostics.contains(expected))
            );
        }
    }
}
