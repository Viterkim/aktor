use aktor::*;
use er::{Er, ErPresentationExt, ErTree};
use std::{io, sync::Arc};

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

async fn open() -> Result<u32, AktorSetupError> {
    Ok(31)
}
fn close(value: u32) -> Result<(), AktorCleanupError> {
    Err(ErTree::new(
        CloseErr { value },
        [
            io::Error::other("flush sentinel"),
            io::Error::other("journal sentinel"),
        ],
    ))
    .er_into(|report| report)
}

#[tokio::test]
async fn direct() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("tree"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: open,
                end: close,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
        },
        shutdown: |_| {},
        options: Default::default(),
    })
    .await
    .unwrap();

    let report = actors.shutdown().await;

    assert_eq!(report.to_string().matches("journal sentinel").count(), 1);

    let error = actors.handles.completion().wait().await.unwrap_err();
    let owner::OwnerFailure::Cleanup(errors) = &error.error else {
        panic!("cleanup error lost");
    };
    let diagnostic = &errors.errors[0].diagnostics;

    assert!(diagnostic.contains("journal sentinel"));
}

#[tokio::test]
async fn rollback() {
    let tree = ErTree::new(OpenErr { value: 17 }, [io::Error::other("open sentinel")]);
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cleanup = cleaned.clone();
    let error = aktor_start(AktorSetup {
        actors: (
            AktorNew {
                name: AktorName::new("ready"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: open,
                    end: async move |value| {
                        cleanup.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        close(value)
                    },
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
            },
            AktorNew {
                name: AktorName::new("failed"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async move || Err::<(), _>(tree).er_into(|report| report),
                    end: (),
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
            },
        ),
        shutdown: |_| {},
        options: Default::default(),
    })
    .await
    .err()
    .unwrap();

    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(format!("{error:#}").matches("open sentinel").count(), 1);
    let wrapped = er::ErTree::from(error);
    assert_eq!(
        wrapped.er_report_string().matches("open sentinel").count(),
        0
    );
    let report = wrapped.top.report.unwrap();

    assert!(report.failure.unwrap().message.contains("open sentinel"));
    assert!(report.actors.iter().any(|actor| {
        actor
            .diagnostics
            .iter()
            .any(|error| error.diagnostics.contains("journal sentinel"))
    }));
}

#[tokio::test]
async fn recovery() {
    let cleanup_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls = cleanup_calls.clone();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("recoverable"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: open,
                end: async move |value| {
                    if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        close(value)
                    } else {
                        Ok(())
                    }
                },
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
        },
        shutdown: |_| {},
        options: Default::default(),
    })
    .await
    .unwrap();
    let controller = actors.handles.control.new_controller();
    let actor = controller.clone();
    let error = controller.replace(|| Ok(41)).await.unwrap_err();
    let listener::ReplaceError::Cleanup(error) = error.error else {
        panic!("replacement lost cleanup classification");
    };

    assert!(error.diagnostics.contains("close database 31"));
    let error = actor
        .resume(|| Err(AktorSetupError::new("resume sentinel", ())))
        .await
        .unwrap_err();
    let listener::LifecycleError::Failed(error) = error.error else {
        panic!("resume lost setup classification");
    };

    assert!(error.diagnostics.contains("resume sentinel"));
    assert!(!actor.is_running());
    assert!(!actors.killswitch().is_stopping());
    controller.resume(|| Ok(43)).await.unwrap();
    let error = actor
        .replace(|| {
            let tree = ErTree::new(OpenErr { value: 44 }, [] as [io::Error; 0]);
            Err(tree).er_into(|report| report)
        })
        .await
        .unwrap_err();
    let listener::ReplaceError::Setup(error) = error.error else {
        panic!("replacement lost setup classification");
    };

    assert!(error.diagnostics.contains("open database 44"));

    use futures_util::FutureExt;
    use tokio::sync::oneshot;

    let (release, gate) = oneshot::channel();
    let mut resume = Box::pin(actor.resume_async(move || async move {
        gate.await.unwrap();
        Err::<u32, _>(AktorSetupError::new("buffered resume error", ()))
    }));

    assert!(resume.as_mut().now_or_never().is_none());
    release.send(()).unwrap();
    actor.pause().await.unwrap();
    drop(resume);

    let (release, gate) = oneshot::channel();
    let mut replace = Box::pin(actor.replace_async(move || async move {
        gate.await.unwrap();
        Err::<u32, _>(AktorSetupError::new("buffered replace error", ()))
    }));

    assert!(replace.as_mut().now_or_never().is_none());
    release.send(()).unwrap();
    actor.pause().await.unwrap();
    drop(replace);

    let failures = actor.take_abandoned_setup();
    let [
        listener::AbandonedSetup::Resume(resume),
        listener::AbandonedSetup::Replace(replace),
    ] = failures.as_slice()
    else {
        panic!("buffered lifecycle failures lost");
    };

    assert_eq!(resume.diagnostics, "buffered resume error");
    assert_eq!(replace.diagnostics, "buffered replace error");

    actor.resume(|| Ok(45)).await.unwrap();
    assert!(!actors.shutdown().await.timed_out);
    assert_eq!(cleanup_calls.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[tokio::test]
async fn task() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("task tree"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: open,
                end: close,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
        },
        shutdown: async |_| {},
        options: Default::default(),
    })
    .await
    .unwrap();
    let report = actors.shutdown().await;

    assert_eq!(report.to_string().matches("journal sentinel").count(), 1);
    assert!(
        actors.handles.cleanup.lock()[0]
            .diagnostics
            .contains("close database 31")
    );
}

#[tokio::test]
async fn reports() {
    let message = "entry sentinel\nfirst cause\nsecond cause";
    let mut group = AktorGroup::new();
    let completed = group
        .start_with(move |_| Err(AktorCleanupError::new(message, ())))
        .unwrap();
    group.shutdown();
    let report = completed.wait().await;

    assert_eq!(report.application[0].diagnostics, message);
    assert_eq!(report.to_string().matches("entry sentinel").count(), 1);

    let report = AktorGroup::new()
        .run(
            async |_| Err::<(), _>(AktorError::new("application sentinel", ())),
            async |_| Err(AktorCleanupError::new("shutdown sentinel", ())),
        )
        .await
        .unwrap_err();

    assert_eq!(
        report.report.application[0].diagnostics,
        "application sentinel"
    );
    assert_eq!(
        report.report.application[1].diagnostics,
        "shutdown sentinel"
    );

    let report = AktorGroup::new()
        .run(
            async |_| -> Result<(), AktorError> { panic!("application panic sentinel") },
            async |_| -> Result<(), AktorCleanupError> { panic!("shutdown panic sentinel") },
        )
        .await
        .unwrap_err();
    let text = &report.diagnostics.report;

    assert_eq!(
        text.matches("application panic sentinel").count(),
        1,
        "{text}"
    );
    assert_eq!(text.matches("shutdown panic sentinel").count(), 1, "{text}");
    assert_eq!(report.report.failure.as_ref().unwrap().phase, "run");

    let standalone = Aktor::spawn(SpawnArgs {
        name: "standalone".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok(31),
        cleanup: close,
    })
    .await
    .unwrap();
    let error = standalone.shutdown().await.unwrap_err();
    let owner::OwnerFailure::Cleanup(errors) = &error.error else {
        panic!("standalone cleanup lost");
    };
    assert!(errors.errors[0].diagnostics.contains("journal sentinel"));

    let standalone = Aktor::spawn(SpawnArgs {
        name: "standalone panic".into(),
        capacity: 1,
        failure: FailurePolicy::Unwind,
        setup: || Ok(31_u32),
        cleanup: |_| Ok::<_, AktorCleanupError>(()),
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

    let error = standalone.completion().await.unwrap_err();
    assert_eq!(error.to_string(), "actor thread failed");
    assert_eq!(
        format!("{error:#}").matches("owner panic sentinel").count(),
        1
    );
}

#[tokio::test]
async fn definition() {
    let definition: AktorDefinition<Aktor<u32>> = AktorNew {
        name: AktorName::new("definition"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok(31),
            end: async |_| Ok(()),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: Default::default(),
    }
    .definition();
    let actors = aktor_start(AktorSetup {
        actors: definition,
        shutdown: |_| {},
        options: Default::default(),
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
                    name: AktorName::new("local payload"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioLocal(&executor),
                    closures: AktorClosures {
                        start: open,
                        end: move |_| {
                            Err(AktorCleanupError::new(format!("local error {payload}"), ()))
                        },
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: Default::default(),
                },
                shutdown: |_| {},
                options: Default::default(),
            })
            .await
            .unwrap();
            let completed = actors.handles.completion();
            let report = actors.shutdown().await;
            assert_eq!(report.to_string().matches("local error 47").count(), 1);
            let error = completed.wait_with_data().await.unwrap_err();
            let local::OwnerFailure::CleanupFailure(error) = &*error.error else {
                panic!("local cleanup value lost");
            };

            assert!(error.diagnostics.contains("local error 47"));

            let startup_payload = original.clone();
            let failed = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: AktorName::new("local startup"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioLocal(&executor),
                    closures: AktorClosures {
                        start: async move || {
                            Err::<(), _>(AktorSetupError::new(
                                format!("local error {startup_payload}"),
                                (),
                            ))
                        },
                        end: (),
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: Default::default(),
                },
                shutdown: |_| {},
                options: Default::default(),
            })
            .await
            .err()
            .expect("local startup succeeded");
            let AktorStartError::Init(error) = &failed.error else {
                panic!("local startup value lost");
            };

            assert!(error.diagnostics.contains("local error 47"));
            assert_eq!(format!("{failed:#}").matches("local error 47").count(), 1);
            assert!(!format!("{failed:?}").contains("local error 47"));
        })
        .await;
}

#[tokio::test]
async fn validation() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn check<Setups, Mode>(actors: Setups, reason: &str, kind: AktorExecution)
    where
        Setups: setup::AktorStart<Mode, Group = AktorGroup>,
    {
        let (seen, callback) = tokio::sync::oneshot::channel();
        let startup = aktor_start(AktorSetup {
            actors,
            shutdown: move |report| {
                seen.send(report).unwrap();
            },
            options: Default::default(),
        });
        let completion = startup.completion();
        let error = startup.await.err().expect("invalid setup succeeded");

        assert!(matches!(
            error.error,
            AktorStartError::Setup(_) | AktorStartError::Thread(_)
        ));
        assert_eq!(format!("{error:#}").matches(reason).count(), 1);

        for report in [callback.await.unwrap(), completion.wait().await] {
            assert!(report.failed(), "{report}");
            assert!(!report.timed_out);
            let failure = report.failure.as_ref().unwrap();

            assert_eq!(failure.kind, Some(kind));
            assert_eq!(failure.phase, "setup");
            assert_eq!(report.to_string().matches(reason).count(), 1);
        }
    }

    for sibling in [false, true] {
        let invalid = AktorNew {
            name: AktorName::new("invalid"),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok(0_u32),
                end: (),
                intervals: vec![AktorInterval {
                    every: std::time::Duration::ZERO,
                    run: (async |_: &mut u32| {}).into(),
                }],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
        };
        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = cleaned.clone();

        if sibling {
            let healthy = AktorNew {
                name: AktorName::new("healthy"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok(()),
                    end: move |()| {
                        cleanup.fetch_add(1, Ordering::SeqCst);
                    },
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
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
            name: AktorName::new("invalid"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(()),
                end: (),
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 0 },
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
                name: AktorName::new("starting"),
                role: AktorNoRole,
                kind: AktorKind::TokioTask,
                closures: AktorClosures {
                    start: async move || {
                        begin.notify_one();
                        gate.notified().await;

                        if panics {
                            panic!("secondary startup sentinel");
                        }

                        Err::<(), _>(AktorSetupError::new("secondary startup sentinel", ()))
                    },
                    end: (),
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
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
            assert!(!report.timed_out);
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
