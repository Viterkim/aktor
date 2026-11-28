use aktor::*;
use core::num::NonZeroUsize;
use er::{Er, ErTree};
use std::{io, sync::Arc, time::Duration};

#[derive(Er)]
#[er(format = "open database {value}")]
struct OpenErr {
    value: u32,
}
#[derive(Er)]
#[er(format = "close database {value}")]
struct CloseErr {
    value: u32,
}

async fn open() -> Result<u32, (core::convert::Infallible, String)> {
    Ok(31)
}
fn close(value: u32) -> Result<(), (CloseErr, String)> {
    Err(ErTree::new(
        CloseErr { value },
        [
            io::Error::new(io::ErrorKind::WriteZero, "flush sentinel"),
            io::Error::new(io::ErrorKind::PermissionDenied, "journal sentinel"),
        ],
    )
    .into())
}

#[tokio::test]
async fn direct() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "tree".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Try(open),
            end: close,
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let completion = actors.handles().completion();
    let (actors, handles) = actors.into_parts();
    drop(handles);
    let report = actors.stop_released().await.unwrap().into_report();
    assert!(report.released());
    assert!(report.to_string().contains("flush sentinel"));
    assert_eq!(report.to_string().matches("journal sentinel").count(), 1);

    let (error, report) = completion.wait().await.unwrap_err();
    let owner::OwnerError::Cleanup = &*error else {
        panic!("cleanup error lost");
    };
    assert!(report.contains("journal sentinel"));
}

#[tokio::test]
async fn rollback() {
    let tree = ErTree::new(OpenErr { value: 17 }, [io::Error::other("open sentinel")]);
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cleanup = cleaned.clone();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: (
            AktorNew {
                name: "ready".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Try(open),
                end: async move |value| {
                    cleanup.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    close(value)
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "ready".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Try(async move || Err::<(), _>(tree)),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });

    let (first, second) = tokio::join!(actors.ready(), actors.ready());
    let (error, error_report) = first.unwrap_err();
    let (repeated, _) = second.unwrap_err();

    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(error_report.matches("open sentinel").count(), 1);
    assert!(core::ptr::eq(error, repeated));

    let id = actors.handles().1.actor_id().unwrap();
    let entry = error
        .startup()
        .unwrap()
        .actors
        .iter()
        .find(|actor| actor.id == id)
        .unwrap();
    assert_eq!(entry.init, AktorInitEvidence::Entered);

    let report = error.startup().unwrap().report.as_ref().unwrap();

    assert!(
        report
            .actor_failures(id)
            .any(|failure| failure.error.contains("open sentinel"))
    );
    assert!(report.to_string().contains("journal sentinel"));
}

#[tokio::test]
async fn task() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "task tree".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Try(open),
            end: close,
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: async |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();
    let report = actors.stop().await;

    assert_eq!(report.to_string().matches("journal sentinel").count(), 1);
}

#[tokio::test]
async fn reports() {
    let failure = ErTree::new(
        CloseErr { value: 1 },
        [io::Error::other(
            "entry sentinel\nfirst cause\nsecond cause",
        )],
    );

    let started = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "report".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || ()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: move |_| Err::<(), _>(failure),
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let report = started.stop().await;

    assert!(report.failures[0].error.contains("entry sentinel"));
    assert_eq!(report.to_string().matches("entry sentinel").count(), 1);
}

#[tokio::test]
async fn definition() {
    fn definition() -> impl AktorDefinition<Aktor<u32>> {
        AktorNew {
            name: "definition".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || 31),
            end: async |_| {},
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        }
    }
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: definition(),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    assert!(!actors.stop().await.failed());
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local() {
    let executor = tokio::task::LocalSet::new();
    executor
        .run_until(async {
            let payload = std::rc::Rc::new(47);
            let original = payload.clone();
            let actors = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "local payload".into(),
                    kind: AktorKind::TokioLocal(&executor),
                    start: AktorInit::Try(open),
                    end: move |_| Err((payload.clone(), format!("local error {payload}"))),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            actors.ready().await.unwrap();
            let completed = actors.handles().completion();
            let report = actors.stop().await;
            assert_eq!(report.to_string().matches("local error 47").count(), 1);
            let (error, completion_text) = completed.wait_with_data().await.unwrap_err();
            let local::OwnerError::CleanupFailure { .. } = &*error.cause else {
                panic!("local cleanup value lost");
            };

            assert!(completion_text.contains("local error 47"));

            let startup_payload = original.clone();
            let attempt = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "local startup".into(),
                    kind: AktorKind::TokioLocal(&executor),
                    start: AktorInit::Try(async move || {
                        Err::<(), _>(((), format!("local error {startup_payload}")))
                    }),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            let (failed, failed_report) =
                attempt.ready().await.expect_err("local startup succeeded");
            assert_eq!(failed.reason(), AktorStartReason::Init);
            assert_eq!(failed_report.matches("local error 47").count(), 1);
        })
        .await;
}

#[tokio::test]
async fn validation() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn check<Setups>(actors: Setups, kind: AktorExecution)
    where
        Setups: AktorStart<Group = AktorGroup>,
    {
        let (seen, callback) = tokio::sync::oneshot::channel();
        let startup = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors,
            shutdown: move |report| {
                seen.send(report).unwrap();
            },
            grace: Duration::from_secs(5),
        });
        let completion = startup.completion();
        let (error, _) = startup.ready().await.expect_err("invalid setup succeeded");

        assert_eq!(error.reason(), AktorStartReason::InvalidSetup);

        for report in [callback.await.unwrap(), completion.wait().await.unwrap()] {
            assert!(report.failed(), "{report}");
            assert!(!report.ran_past_grace());
            let failure = &report.failures[0];

            assert!(
                matches!(failure.source, aktor::AktorFailureSource::Actor { kind: execution, .. } if execution == kind)
            );
            assert!(matches!(
                failure.source,
                aktor::AktorFailureSource::Actor {
                    phase: aktor::AktorPhase::Start,
                    ..
                }
            ));
        }
    }

    for sibling in [false, true] {
        let invalid = AktorNew {
            name: "invalid".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || 0_u32),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MAX,
            stuck: AktorStuck::Kill,
        };
        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = cleaned.clone();

        if sibling {
            let healthy = AktorNew {
                name: "healthy".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Ready(async || ()),
                end: move |()| {
                    cleanup.fetch_add(1, Ordering::SeqCst);
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            };

            check((healthy, invalid), AktorExecution::TokioThread).await;
            assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        } else {
            check(invalid, AktorExecution::TokioThread).await;
        }
    }

    check(
        AktorNew {
            name: "invalid".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || ()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(usize::MAX).unwrap(),
            stuck: AktorStuck::Kill,
        },
        AktorExecution::TokioTask,
    )
    .await;
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn startup_during_shutdown() {
    use tokio::sync::Notify;

    for panics in [false, true] {
        let (seen, callback) = tokio::sync::oneshot::channel();
        let started = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "running".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Ready(async || ()),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: move |report| {
                seen.send(report).unwrap();
            },
            grace: Duration::from_secs(1),
        });
        started.ready().await.unwrap();
        let completion = started.completion();
        let actor = started.handles();
        let group = started.group();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let gate = release.clone();
        let begin = entered.clone();
        let spawning = group.spawn(AktorNew {
            name: "starting".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Try(async move || {
                begin.notify_one();
                gate.notified().await;

                if panics {
                    panic!("secondary startup sentinel");
                }

                Err::<(), _>(((), String::from("secondary startup sentinel")))
            }),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        });
        let mut startup = Box::pin(spawning.ready());

        tokio::select! {
            _ = &mut startup => panic!("startup escaped its gate"),
            _ = entered.notified() => {},
        }

        aktor::testing::call(actor, async |_| panic!("primary operation sentinel"))
            .cast()
            .await
            .unwrap();
        group.killswitch().wait_stopping().await;
        release.notify_one();
        assert!(startup.await.is_err());

        for report in [callback.await.unwrap(), completion.wait().await.unwrap()] {
            assert!(
                matches!(&report.failures[0].source, aktor::AktorFailureSource::Actor { name, .. } if name == "running")
            );
            assert!(!report.ran_past_grace());
            assert!(report.failures.iter().any(|failure| {
                matches!(&failure.source, aktor::AktorFailureSource::Actor { name, .. } if name == "starting")
                    && failure.error.contains("secondary startup sentinel")
            }));
            assert_eq!(
                report
                    .to_string()
                    .matches("secondary startup sentinel")
                    .count(),
                1
            );
        }
    }
}
