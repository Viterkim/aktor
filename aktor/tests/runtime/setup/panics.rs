use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "std_thread")]
#[tokio::test]
async fn native() {
    let mut missing = Vec::new();

    for standard in [false, true] {
        let mut group = AktorGroup::new();
        let completion = if standard {
            group.start_standard().unwrap()
        } else {
            group.start_threaded().unwrap()
        };
        let cleaned = Arc::new(AtomicUsize::new(0));
        let count = cleaned.clone();
        let actor = group
            .spawn(ActorArgs::new(
                "failed operation",
                || Ok::<_, AktorSetupError>(0u32),
                move |_| -> Result<(), AktorCleanupError> {
                    count.fetch_add(1, Ordering::SeqCst);
                    panic!("secondary cleanup sentinel");
                },
            ))
            .await
            .unwrap();

        message::call(
            &actor.handle,
            |_: &mut u32, ()| panic!("primary operation sentinel"),
            (),
        )
        .cast()
        .await;
        let report = tokio::time::timeout(Duration::from_secs(3), completion.wait())
            .await
            .unwrap();

        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(
            report
                .to_string()
                .matches("primary operation sentinel")
                .count(),
            1
        );
        assert!(
            report
                .failure
                .as_ref()
                .unwrap()
                .message
                .contains("primary operation sentinel")
        );
        if !report.to_string().contains("secondary cleanup sentinel") {
            missing.push(format!(
                "standard={standard}: missing secondary cleanup panic"
            ));
        }
        let completed = actor.completion().wait().await.unwrap_err();
        if !completed.to_string().contains("secondary cleanup sentinel") {
            missing.push(format!(
                "standard={standard}: missing completion diagnostic"
            ));
        }

        let cleaned = Arc::new(AtomicUsize::new(0));
        let count = cleaned.clone();
        macro_rules! rollback {
            ($kind:expr) => {
                aktor_start(AktorSetup {
                    actors: (
                        AktorNew {
                            name: AktorName::new("ready"),
                            role: AktorNoRole,
                            kind: $kind,
                            closures: AktorClosures {
                                start: async || Ok::<_, AktorSetupError>(()),
                                end: Some(
                                    (async move |_| -> Result<(), AktorCleanupError> {
                                        count.fetch_add(1, Ordering::SeqCst);
                                        panic!("rollback panic sentinel");
                                    })
                                    .into(),
                                ),
                                intervals: vec![],
                                before_each: None,
                                after_each: None,
                            },
                            options: Default::default(),
                        },
                        AktorNew {
                            name: AktorName::new("failed setup"),
                            role: AktorNoRole,
                            kind: $kind,
                            closures: AktorClosures {
                                start: async || {
                                    Err::<(), _>(AktorSetupError::new("sibling setup sentinel"))
                                },
                                end: None,
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
                .unwrap()
            };
        }
        let error = if standard {
            rollback!(AktorKind::StdThread)
        } else {
            rollback!(AktorKind::TokioThread)
        };
        let report = error.report.as_ref().unwrap();

        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        assert!(
            report
                .failure
                .as_ref()
                .unwrap()
                .message
                .contains("sibling setup sentinel")
        );
        if !report.actors.iter().any(|actor| {
            actor.actor == "ready"
                && actor
                    .diagnostics
                    .iter()
                    .any(|error| error.diagnostics.contains("rollback panic sentinel"))
        }) {
            missing.push(format!("standard={standard}: missing rollback panic"));
        }
    }

    assert!(missing.is_empty(), "{missing:?}");
}

#[aktor]
async fn gated(_: &mut u32, entered: Arc<Notify>, release: Arc<Notify>, message: &'static str) {
    entered.notify_one();
    release.notified().await;
    panic!("{message}");
}

#[tokio::test]
async fn siblings() {
    let mut missing = Vec::new();

    macro_rules! check {
        ($kind:expr) => {{
            macro_rules! actor {
                ($name:expr) => {
                    AktorNew {
                        name: AktorName::new($name),
                        role: AktorNoRole,
                        kind: $kind,
                        closures: AktorClosures {
                            start: async || Ok::<_, AktorSetupError>(0u32),
                            end: None,
                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: Default::default(),
                    }
                };
            }
            let actors = aktor_start(AktorSetup {
                actors: (actor!("first"), actor!("later")),
                shutdown: |_| {},
                options: Default::default(),
            })
            .await
            .unwrap();
            let entered_a = Arc::new(Notify::new());
            let entered_b = Arc::new(Notify::new());
            let release_a = Arc::new(Notify::new());
            let release_b = Arc::new(Notify::new());
            let first = gated(
                &actors.handles.0,
                entered_a.clone(),
                release_a.clone(),
                "first actor sentinel",
            )
            .send()
            .await;
            let later = gated(
                &actors.handles.1,
                entered_b.clone(),
                release_b.clone(),
                "later actor sentinel",
            )
            .send()
            .await;

            tokio::time::timeout(Duration::from_secs(3), async {
                entered_a.notified().await;
                entered_b.notified().await;
                release_a.notify_one();
                actors.killswitch().wait_stopping().await;
            })
            .await
            .unwrap();
            release_b.notify_one();
            let report = tokio::time::timeout(Duration::from_secs(3), actors.completion().wait())
                .await
                .unwrap();

            assert!(
                report
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("first actor sentinel")
            );
            assert_eq!(
                report.to_string().matches("first actor sentinel").count(),
                1
            );
            if !report.actors.iter().any(|actor| {
                actor.actor == "later"
                    && actor
                        .diagnostics
                        .iter()
                        .any(|error| error.diagnostics.contains("later actor sentinel"))
            }) {
                missing.push(stringify!($kind));
            }
            drop((first, later));
        }};
    }

    let executor = tokio::task::LocalSet::new();
    executor
        .run_until(async {
            check!(AktorKind::TokioTask);
            check!(AktorKind::TokioThread);
            #[cfg(feature = "std_thread")]
            check!(AktorKind::StdThread);
            #[cfg(feature = "local")]
            check!(AktorKind::TokioLocal(&executor));
        })
        .await;

    assert!(missing.is_empty(), "{missing:?}");
}

#[cfg(feature = "local")]
#[test]
fn closures() {
    if std::env::var("AKTOR_CHILD").as_deref() != Ok("closure drops") {
        let output = support::child("setup::panics::closures", "closure drops");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    struct Broken(Rc<Cell<u32>>, u32, &'static str);
    impl Drop for Broken {
        fn drop(&mut self) {
            self.0.set(self.0.get() | self.1);
            panic!("{}", self.2);
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (_, owner) = local::channel::<(), 1, ()>().unwrap();
        let completion = owner.completion();
        let dropped = Rc::new(Cell::new(0));
        let cleanup = Broken(dropped.clone(), 1, "cleanup capture sentinel");
        let runner = Broken(dropped.clone(), 2, "runner capture sentinel");
        let error = owner
            .run_with_custom(
                async || Err(AktorSetupError::new("initialization refused")),
                async move |_| {
                    std::hint::black_box(&cleanup);
                    Ok(())
                },
                async move |_: local::AktorRunner<'_, (), 1, ()>| {
                    std::hint::black_box(&runner);
                    Ok(())
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, local::OwnerError::Setup(_)));
        assert_eq!(dropped.get(), 3);
        let diagnostics = format!("{:?}", completion.diagnostics());
        assert!(diagnostics.contains("cleanup capture sentinel"));
        assert!(diagnostics.contains("runner capture sentinel"));
    });
}
