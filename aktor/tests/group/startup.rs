use super::*;
use core::num::NonZeroUsize;

#[cfg(feature = "std_thread")]
#[test]
fn mixed_group_runtime() {
    use aktor::{AktorHooks, AktorKind, AktorNew, AktorSetup, AktorStuck, aktor_start};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let actors = runtime.block_on(async {
        let actors = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: (
                AktorNew {
                    name: "standard".into(),
                    kind: AktorKind::StdThread,
                    start: aktor::AktorInit::Ready(async || ()),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::MIN,
                    stuck: AktorStuck::Kill,
                },
                AktorNew {
                    name: "tokio".into(),
                    kind: AktorKind::TokioThread,
                    start: aktor::AktorInit::Ready(async || ()),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::MIN,
                    stuck: AktorStuck::Kill,
                },
            ),
            shutdown: |_| {},
            grace: Duration::from_secs(1),
        });

        actors.ready().await.unwrap();
        actors
    });

    drop(runtime);

    aktor::executor::block_on(actors.group().spawn(support::thread_actor((), 1)).ready()).unwrap();
}

#[tokio::test]
async fn startup_failure() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let closed = cleaned.clone();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "opened resource".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || ()),
            end: move |_| {
                closed.store(true, Ordering::SeqCst);
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let completed = started.completion();
    let group = started.group();

    let attempt = group.spawn(AktorNew {
        name: "failed setup".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Try(async || Err::<(), _>(((), String::from("setup went wrong")))),
        end: |_| (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    });
    assert!(attempt.ready().await.is_err());

    let report = completed.wait().await.unwrap();

    assert!(matches!(
        report.stop_cause,
        aktor::StopCause::StartupFailure(_)
    ));
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(matches!(
        report.failures[0].source,
        aktor::AktorFailureSource::Actor {
            phase: aktor::AktorPhase::Start,
            ..
        }
    ));
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
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let inside = entered.clone();
        let setup_release = release.clone();
        let started = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "cancelled startup".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Try(async move || {
                    inside.notify_one();
                    setup_release.notified().await;

                    if setup_fails {
                        Err(((), String::from("late setup error")))
                    } else {
                        Ok(())
                    }
                }),
                end: async |_| Err(((), String::from("late cleanup error"))),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::MIN,
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(1),
        });
        let kill = started.killswitch();
        let completed = started.completion();
        let stop = kill.clone();
        let trigger = tokio::spawn(async move {
            entered.notified().await;
            stop.stop();
        });
        kill.run(async move {
            let _release = ReleaseOnDrop(release);
            started.ready().await.unwrap();
        })
        .await;
        let result = completed.wait().await.unwrap();

        trigger.await.unwrap();

        assert!(!result.ran_past_grace());

        let expected = if setup_fails {
            "late setup error"
        } else {
            "late cleanup error"
        };

        let failure = &result.failures[0];

        let expected_phase = if setup_fails {
            aktor::AktorPhase::Start
        } else {
            aktor::AktorPhase::End
        };
        assert!(
            matches!(&failure.source, aktor::AktorFailureSource::Actor { phase, .. } if phase == &expected_phase)
        );
        assert_eq!(result.to_string().matches(expected).count(), 1);
    }
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn startup() {
    for fail in [false, true] {
        let hooks = Arc::new(AtomicUsize::new(0));
        let hook_count = hooks.clone();
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleanup = cleaned.clone();
        let setup = aktor::AktorInit::Ready(async || 0usize);
        let cleanup = move |_| {
            cleanup.store(true, Ordering::SeqCst);
        };
        let started = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "database".into(),
                kind: AktorKind::TokioThread,
                start: setup,
                end: cleanup,
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: move |_| {
                hook_count.fetch_add(1, Ordering::SeqCst);
            },
            grace: Duration::from_secs(1),
        });
        started.ready().await.unwrap();
        let kill = started.killswitch();
        let completed = started.completion();
        let closing = started.completion();
        let (actors, database) = started.into_parts();
        let caller_kill = kill.clone();
        let started = Arc::new(Notify::new());
        let caller_started = started.clone();
        let caller = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = caller_kill.wait_stopping() => {},
                _ = async {
                    let result = call(&database, async |_| Err::<(), _>("query failed")).await;
                    assert_eq!(result, Ok(Err("query failed")));
                    assert!(!caller_kill.is_stopping());
                    if fail {
                        caller_started.notify_one();
                        call(&database, async |_| -> () { panic!("storage died") }).await.unwrap_err();
                    } else {
                        caller_started.notify_one();
                        core::future::pending::<()>().await;
                    }
                } => {},
            }
        });

        started.notified().await;

        if !fail {
            kill.stop();
        }

        let report = tokio::time::timeout(Duration::from_secs(2), closing.wait())
            .await
            .unwrap()
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), caller)
            .await
            .unwrap()
            .unwrap();
        assert!(cleaned.load(Ordering::SeqCst));
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        assert_eq!(report.failed(), fail);
        assert_eq!(completed.wait().await.unwrap().failed(), fail);

        if fail {
            assert!(report.failures[0].error.contains("storage died"));
        }

        let late = AktorNew {
            name: "late".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || ()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        };

        assert!(actors.group().spawn(late).ready().await.is_err());
    }
}

#[tokio::test]
async fn default_group() {
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "counter".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async move || 17usize),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    actors.ready().await.unwrap();
    let closing = actors.completion();
    let actor = actors.handles();
    let shutdown = actors.stop();

    assert!(actors.killswitch().is_stopping());
    closing.await.unwrap().into_result().unwrap();
    shutdown.await.into_result().unwrap();
    assert!(actor.completion().wait().await.is_ok());
}

#[tokio::test]
async fn startup_rollback() {
    let hooks = Arc::new(AtomicBool::new(false));
    let hook = hooks.clone();
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup = cleaned.clone();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "audio".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || ()),
            end: move |_| {
                cleanup.store(true, Ordering::SeqCst);
                Err(((), String::from("audio cleanup failed")))
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: move |_| {
            hook.store(true, Ordering::SeqCst);
        },
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let closing = started.completion();
    let startup = tokio::spawn(async move {
        let attempt = started.group().spawn(AktorNew {
            name: "storage".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Try(async || {
                Err::<(), _>(((), String::from("could not open")))
            }),
            end: |_| (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        });
        attempt.ready().await.err().unwrap().1
    });

    let diagnostics = tokio::time::timeout(Duration::from_secs(2), startup)
        .await
        .unwrap()
        .unwrap();

    for message in ["could not open", "audio cleanup failed"] {
        assert_eq!(diagnostics.matches(message).count(), 1, "{diagnostics}");
    }

    assert!(cleaned.load(Ordering::SeqCst));
    assert!(hooks.load(Ordering::SeqCst));

    let report = closing.wait().await.unwrap();

    assert!(
        matches!(&report.failures[0].source, aktor::AktorFailureSource::Actor { name, .. } if name == "storage")
    );
}

#[tokio::test]
async fn listener_cancel() {
    for setup_fails in [false, true] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let setup_entered = entered.clone();
        let setup_release = release.clone();
        let started = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "storage".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Try(async move || {
                    setup_entered.notify_one();
                    setup_release.notified().await;
                    if setup_fails {
                        Err(((), String::from("late setup failed")))
                    } else {
                        Ok(())
                    }
                }),
                end: async |_| Err::<(), _>(((), String::from("late cleanup failed"))),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(1),
        });
        let kill = started.killswitch();
        let closing = started.completion();
        let startup = tokio::spawn(async move { started.ready().await.is_err() });

        entered.notified().await;
        kill.stop();
        startup.abort();
        assert!(startup.await.err().unwrap().is_cancelled());
        release.notify_one();

        let report = tokio::time::timeout(Duration::from_secs(2), closing.wait())
            .await
            .unwrap()
            .unwrap();
        let expected = if setup_fails {
            "late setup failed"
        } else {
            "late cleanup failed"
        };

        assert!(report.failed());

        let failure = &report.failures[0];
        let expected_phase = if setup_fails {
            aktor::AktorPhase::Start
        } else {
            aktor::AktorPhase::End
        };
        assert!(
            matches!(&failure.source, aktor::AktorFailureSource::Actor { phase, .. } if phase == &expected_phase)
        );
        assert_eq!(report.to_string().matches(expected).count(), 1);
    }
}
