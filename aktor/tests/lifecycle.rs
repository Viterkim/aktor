use aktor::listener::{FailurePolicy, SpawnArgs};
use aktor::*;
use er::{Er, ErPresentationExt, ErResult, ErTree};
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

async fn open() -> Result<u32, String> {
    Ok(31)
}
fn close(value: u32) -> ErResult<(), CloseErr> {
    Err(ErTree::new(
        CloseErr { value },
        [
            io::Error::new(io::ErrorKind::WriteZero, "flush sentinel"),
            io::Error::new(io::ErrorKind::PermissionDenied, "journal sentinel"),
        ],
    ))
}

#[tokio::test]
async fn direct() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "tree".into(),
            kind: AktorKind::TokioThread,
            start: open,
            end: close,
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .unwrap();

    let completion = actors.handles.completion();
    drop(actors.handles);
    let report = actors.group.shutdown().wait_retired().await.unwrap();
    assert!(report.retired());
    assert!(report.to_string().contains("flush sentinel"));
    assert_eq!(report.to_string().matches("journal sentinel").count(), 1);

    let (error, report) = completion.wait().await.unwrap_err();
    let owner::OwnerError::Cleanup(errors) = &*error else {
        panic!("cleanup error lost");
    };
    assert_eq!(errors.errors.len(), 1);
    assert!(report.contains("journal sentinel"));
}

#[tokio::test]
async fn rollback() {
    let tree = ErTree::new(OpenErr { value: 17 }, [io::Error::other("open sentinel")]);
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cleanup = cleaned.clone();
    let (error, error_report) = aktor_start(AktorSetup {
        actors: (
            AktorNew {
                name: "ready".into(),
                kind: AktorKind::TokioThread,
                start: open,
                end: async move |value| {
                    cleanup.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    close(value)
                },
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "failed".into(),
                kind: AktorKind::TokioThread,
                start: async move || Err::<(), _>(tree).er_report(),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .err()
    .unwrap();

    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(error_report.matches("open sentinel").count(), 1);
    let wrapped = er::ErTree::from(error);
    assert_eq!(wrapped.er_report().matches("open sentinel").count(), 0);
    let report = wrapped.top.report.unwrap();

    assert!(
        report
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("open sentinel")
    );
    assert!(report.to_string().contains("journal sentinel"));
}

#[tokio::test]
async fn task() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "task tree".into(),
            kind: AktorKind::TokioTask,
            start: open,
            end: close,
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: async |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .unwrap();
    let report = actors.shutdown().await;

    assert_eq!(report.to_string().matches("journal sentinel").count(), 1);
}

#[tokio::test]
async fn reports() {
    let message = "entry sentinel\nfirst cause\nsecond cause";
    let mut group = AktorGroup::new();
    let completed = group
        .start_with(move |_| Err(String::from(message)))
        .unwrap();
    group.shutdown();
    let report = completed.wait().await;

    assert_eq!(report.application[0].diagnostics, message);
    assert_eq!(report.to_string().matches("entry sentinel").count(), 1);

    let standalone = Aktor::spawn(SpawnArgs {
        name: "standalone".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok(31),
        cleanup: close,
    })
    .await
    .unwrap();
    let (error, report) = standalone.shutdown().await.unwrap_err();
    let owner::OwnerError::Cleanup(errors) = &*error else {
        panic!("standalone cleanup lost");
    };
    assert_eq!(errors.errors.len(), 1);
    assert!(report.contains("journal sentinel"));

    let standalone = Aktor::spawn(SpawnArgs {
        name: "standalone panic".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok(31_u32),
        cleanup: |_| Ok::<_, String>(()),
    })
    .await
    .unwrap();
    message::call(
        &standalone.handle,
        |_: &mut u32, ()| panic!("owner panic sentinel"),
        (),
    )
    .cast()
    .await;

    let (error, report) = standalone.completion().await.unwrap_err();
    assert_eq!(error.to_string(), "actor thread failed");
    assert_eq!(report.matches("owner panic sentinel").count(), 1);
}

#[tokio::test]
async fn definition() {
    fn definition() -> impl AktorDefinition<Aktor<u32>> {
        AktorNew {
            name: "definition".into(),
            kind: AktorKind::TokioThread,
            start: async || aktor_ready(31),
            end: async |_| {},
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        }
    }
    let actors = aktor_start(AktorSetup {
        actors: definition(),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .unwrap();

    assert!(!actors.shutdown().await.failed());
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
                actors: AktorNew {
                    name: "local payload".into(),
                    kind: AktorKind::TokioLocal(&executor),
                    start: open,
                    end: move |_| Err(format!("local error {payload}")),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
            .await
            .unwrap();
            let completed = actors.handles.completion();
            let report = actors.shutdown().await;
            assert_eq!(report.to_string().matches("local error 47").count(), 1);
            let (error, completion_text) = completed.wait_with_data().await.unwrap_err();
            let local::OwnerError::CleanupFailure(error) = &*error.cause else {
                panic!("local cleanup value lost");
            };

            assert!(error.diagnostics.contains("local error 47"));
            assert!(completion_text.contains("local error 47"));

            let startup_payload = original.clone();
            let (failed, failed_report) = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: "local startup".into(),
                    kind: AktorKind::TokioLocal(&executor),
                    start: async move || Err::<(), _>(format!("local error {startup_payload}")),
                    end: (),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
            .await
            .err()
            .expect("local startup succeeded");
            let AktorStartCause::Init(error) = &failed.cause else {
                panic!("local startup value lost");
            };

            assert!(error.diagnostics.contains("local error 47"));
            assert_eq!(failed_report.matches("local error 47").count(), 1);
        })
        .await;
}

#[tokio::test]
async fn validation() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn check<Setups>(actors: Setups, reason: &str, kind: AktorExecution)
    where
        Setups: setup::AktorStart<Group = AktorGroup>,
    {
        let (seen, callback) = tokio::sync::oneshot::channel();
        let startup = aktor_start(AktorSetup {
            actors,
            shutdown: move |report| {
                seen.send(report).unwrap();
            },
            grace: Duration::from_secs(5),
        });
        let completion = startup.completion();
        let (error, error_report) = startup.await.err().expect("invalid setup succeeded");

        assert!(matches!(
            error.cause,
            AktorStartCause::Setup(_) | AktorStartCause::Thread(_)
        ));
        assert_eq!(error_report.matches(reason).count(), 1);

        for report in [callback.await.unwrap(), completion.wait().await] {
            assert!(report.failed(), "{report}");
            assert!(!report.ran_past_grace);
            let failure = report.failure.as_ref().unwrap();

            assert_eq!(failure.kind, Some(kind));
            assert_eq!(failure.phase, "setup");
            assert_eq!(report.to_string().matches(reason).count(), 1);
        }
    }

    for sibling in [false, true] {
        let invalid = AktorNew {
            name: "invalid".into(),
            kind: AktorKind::TokioThread,
            start: async || aktor_ready(0_u32),
            end: (),
            hooks: AktorHooks {
                intervals: vec![AktorInterval {
                    every: std::time::Duration::ZERO,
                    run: (async |_: &mut u32| {}).into(),
                }],
                before_each: None,
                after_each: None,
            },
            capacity: 32,
            stuck: AktorStuck::Kill,
        };
        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = cleaned.clone();

        if sibling {
            let healthy = AktorNew {
                name: "healthy".into(),
                kind: AktorKind::TokioThread,
                start: async || aktor_ready(()),
                end: move |()| {
                    cleanup.fetch_add(1, Ordering::SeqCst);
                },
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            };

            check(
                (healthy, invalid),
                "interval duration must be positive",
                AktorExecution::TokioThread,
            )
            .await;
            assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        } else {
            check(
                invalid,
                "interval duration must be positive",
                AktorExecution::TokioThread,
            )
            .await;
        }
    }

    check(
        AktorNew {
            name: "invalid".into(),
            kind: AktorKind::TokioTask,
            start: async || aktor_ready(()),
            end: (),
            hooks: Default::default(),
            capacity: 0,
            stuck: AktorStuck::Kill,
        },
        "queue capacity must be positive",
        AktorExecution::TokioTask,
    )
    .await;
}

#[tokio::test]
async fn startup_during_shutdown() {
    use tokio::sync::Notify;

    for panics in [false, true] {
        let (seen, callback) = tokio::sync::oneshot::channel();
        let mut group = AktorGroup::new();
        let completion = group
            .start_with(move |report| {
                seen.send(report).unwrap();
            })
            .unwrap();
        let actor = group.spawn_value("running", ()).await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let gate = release.clone();
        let begin = entered.clone();
        let mut startup = Box::pin(aktor_start_in(
            &group,
            AktorNew {
                name: "starting".into(),
                kind: AktorKind::TokioTask,
                start: async move || {
                    begin.notify_one();
                    gate.notified().await;

                    if panics {
                        panic!("secondary startup sentinel");
                    }

                    Err::<(), _>(String::from("secondary startup sentinel"))
                },
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
        ));

        tokio::select! {
            _ = &mut startup => panic!("startup escaped its gate"),
            _ = entered.notified() => {},
        }

        message::call(
            &actor.handle,
            |_, ()| panic!("primary operation sentinel"),
            (),
        )
        .cast()
        .await;
        group.killswitch().wait_stopping().await;
        release.notify_one();
        assert!(startup.await.is_err());

        for report in [callback.await.unwrap(), completion.wait().await] {
            assert_eq!(report.failure.as_ref().unwrap().actor, "running");
            assert!(!report.ran_past_grace);
            let actor = report
                .actors
                .iter()
                .find(|actor| actor.actor == "starting")
                .unwrap();

            assert_eq!(
                actor
                    .diagnostics
                    .iter()
                    .filter(|error| error.diagnostics.contains("secondary startup sentinel"))
                    .count(),
                1
            );
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
