use super::*;
use core::num::NonZeroUsize;
use std::time::Duration;
#[cfg(feature = "std_thread")]
use std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "std_thread")]
#[derive(Debug)]
struct FormatPanic(bool);

#[cfg(feature = "std_thread")]
impl fmt::Display for FormatPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 {
            f.write_str("source panic context")
        } else {
            panic!("rollback panic sentinel");
        }
    }
}

#[cfg(feature = "std_thread")]
impl Error for FormatPanic {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if self.0 {
            panic!("source panic sentinel");
        }

        None
    }
}

#[cfg(feature = "std_thread")]
#[aktor]
async fn fail_operation(_: &u32) {
    panic!("primary operation sentinel");
}

#[cfg(feature = "std_thread")]
#[tokio::test]
async fn native_cleanup_primary() {
    struct Hook;
    impl Drop for Hook {
        fn drop(&mut self) {
            panic!("hook drop died");
        }
    }

    for standard in [false, true] {
        let hook = Hook;

        macro_rules! actor {
            ($kind:expr) => {
                AktorNew {
                    name: "failed cleanup".into(),
                    kind: $kind,
                    start: aktor::AktorInit::Ready(async || ()),
                    end: async |_| Err::<(), _>(((), String::from("cleanup refused"))),
                    hooks: AktorHooks {
                        intervals: vec![],
                        before_each: move |_: &mut (), _| {
                            let _retained = &hook;
                        },
                        after_each: (),
                    },
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                }
            };
        }

        let group = if standard {
            aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: actor!(AktorKind::StdThread),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
        } else {
            aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: actor!(AktorKind::TokioThread),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
        };
        group.ready().await.unwrap();

        let report = group.stop().await;

        assert!(matches!(
            report.failures[0].source,
            aktor::AktorFailureSource::Actor {
                phase: aktor::AktorPhase::End,
                ..
            }
        ));
        assert!(report.to_string().contains("hook drop died"));
    }
}

#[cfg(feature = "std_thread")]
#[tokio::test]
async fn native() {
    let mut missing = Vec::new();

    for standard in [false, true] {
        let cleaned = Arc::new(AtomicUsize::new(0));
        let count = cleaned.clone();
        let end = move |_| -> () {
            count.fetch_add(1, Ordering::SeqCst);
            panic!("secondary cleanup sentinel");
        };
        let start = aktor::AktorInit::Ready(async || 0u32);
        macro_rules! launch {
            ($kind:expr) => {
                aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::KeepRunning,
                    actors: AktorNew {
                        name: "failed operation".into(),
                        kind: $kind,
                        start,
                        end,
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                })
            };
        }
        let started = if standard {
            launch!(AktorKind::StdThread)
        } else {
            launch!(AktorKind::TokioThread)
        };
        started.ready().await.unwrap();
        let completion = started.completion();
        let actor = started.handles();
        fail_operation(actor).cast().await.unwrap();
        let report = tokio::time::timeout(Duration::from_secs(3), completion.wait())
            .await
            .unwrap()
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
            report.failures[0]
                .error
                .contains("primary operation sentinel")
        );
        if !report.to_string().contains("secondary cleanup sentinel") {
            missing.push(format!(
                "standard={standard}: missing secondary cleanup panic"
            ));
        }
        let (_, report) = actor.completion().wait().await.unwrap_err();
        if !report.contains("secondary cleanup sentinel") {
            missing.push(format!(
                "standard={standard}: missing completion diagnostic"
            ));
        }

        let cleaned = Arc::new(AtomicUsize::new(0));
        let count = cleaned.clone();
        macro_rules! rollback {
            ($kind:expr) => {
                aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::KeepRunning,
                    actors: (
                        AktorNew {
                            name: "ready".into(),
                            kind: $kind,
                            start: aktor::AktorInit::Ready(async || ()),
                            end: async move |_| {
                                count.fetch_add(1, Ordering::SeqCst);
                                Err(((), format!("{}", FormatPanic(false))))
                            },
                            hooks: AktorHooks::NONE,
                            capacity: NonZeroUsize::new(32).unwrap(),
                            stuck: AktorStuck::Kill,
                        },
                        AktorNew {
                            name: "failed setup".into(),
                            kind: $kind,
                            start: aktor::AktorInit::Try(async || {
                                Err::<(), _>(((), format!("{}", FormatPanic(false))))
                            }),
                            end: (),
                            hooks: AktorHooks::NONE,
                            capacity: NonZeroUsize::new(32).unwrap(),
                            stuck: AktorStuck::Kill,
                        },
                    ),
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                })
            };
        }
        let attempt = if standard {
            rollback!(AktorKind::StdThread)
        } else {
            rollback!(AktorKind::TokioThread)
        };
        let (error, error_report) = attempt.ready().await.err().unwrap();
        let printed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (error_report.clone(), format!("{error:?}"))
        }))
        .expect("public startup formatting panicked");
        assert!(printed.1.starts_with("actor startup failed"));
        assert!(printed.0.contains("failed setup"));
        assert_eq!(printed.0.matches("rollback panic sentinel").count(), 2);

        let report = error.startup().unwrap().report.as_ref().unwrap();

        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        assert!(report.failures[0].error.contains("rollback panic sentinel"));
        if !report.failures.iter().any(|failure| matches!(&failure.source, aktor::AktorFailureSource::Actor { name, .. } if name == "ready") && failure.error.contains("rollback panic sentinel")) {
            missing.push(format!("standard={standard}: missing rollback panic"));
        }
    }

    for source in [false, true] {
        let attempt = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "hostile startup".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Try(async move || {
                    let error = FormatPanic(source);
                    error.source();
                    Err::<(), _>(((), format!("{error}")))
                }),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });
        let (error, error_report) = attempt.ready().await.err().unwrap();
        let printed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (error_report.clone(), format!("{error:?}"))
        }))
        .expect("public startup formatting panicked");

        assert!(printed.1.starts_with("actor startup failed"));
        assert!(printed.0.contains("hostile startup"));
        let sentinel = if source {
            "source panic sentinel"
        } else {
            "rollback panic sentinel"
        };
        assert_eq!(printed.0.matches(sentinel).count(), 1);
        assert!(
            error.startup().unwrap().report.as_ref().unwrap().failures[0]
                .error
                .contains(sentinel)
        );
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
                        name: $name.into(),
                        kind: $kind,
                        start: aktor::AktorInit::Ready(async || 0u32),
                        end: (),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    }
                };
            }
            let actors = aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: (actor!("first"), actor!("later")),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            actors.ready().await.unwrap();
            let entered_a = Arc::new(Notify::new());
            let entered_b = Arc::new(Notify::new());
            let release_a = Arc::new(Notify::new());
            let release_b = Arc::new(Notify::new());
            let first = gated(
                &actors.handles().0,
                entered_a.clone(),
                release_a.clone(),
                "first actor sentinel",
            )
            .send()
            .await.unwrap();
            let later = gated(
                &actors.handles().1,
                entered_b.clone(),
                release_b.clone(),
                "later actor sentinel",
            )
            .send()
            .await.unwrap();

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
                .unwrap().unwrap();

            assert!(
                report
                    .failures[0]
                    .error
                    .contains("first actor sentinel")
            );
            assert_eq!(
                report.to_string().matches("first actor sentinel").count(),
                1
            );
            if !report.failures.iter().any(|failure| matches!(&failure.source, aktor::AktorFailureSource::Actor { name, .. } if name == "later") && failure.error.contains("later actor sentinel")) {
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
    if std::env::var("AKTOR_TEST_CASE").as_deref() != Ok("closure drops") {
        let output = support::run_test("setup::panics::closures", "closure drops");
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
        let (_, owner) = local::channel::<(), ()>(NonZeroUsize::MIN);
        let completion = owner.completion();
        let dropped = Rc::new(Cell::new(0));
        let cleanup = Broken(dropped.clone(), 1, "cleanup capture sentinel");
        let runner = Broken(dropped.clone(), 2, "runner capture sentinel");
        let error = owner
            .run_with_custom(
                async || Err(((), "initialization refused".into())),
                async move |_| {
                    std::hint::black_box(&cleanup);
                    Ok(())
                },
                async move |_: local::AktorRunner<'_, ()>| {
                    std::hint::black_box(&runner);
                    Ok(())
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error.0, local::OwnerError::Setup(_)));
        assert_eq!(dropped.get(), 3);
        let diagnostics = completion.wait().await.unwrap_err().1;
        assert!(diagnostics.contains("cleanup capture sentinel"));
        assert!(diagnostics.contains("runner capture sentinel"));
    });
}
