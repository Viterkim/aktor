use super::*;
use aktor::setup::closures::AktorIntervalLogic;
use core::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct Broken(Arc<AtomicUsize>);
impl Drop for Broken {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("independent destructor");
    }
}

#[test]
fn teardown() {
    let Ok(mode) = std::env::var("AKTOR_TEST_CASE") else {
        for mode in [
            "local queue",
            "local hooks",
            "cross queue",
            "cross hooks",
            "local cancelled",
            "cross cancelled",
            "task queue",
            "task hooks",
            "task before",
            "task latest before",
            "local intervals",
            "cross intervals",
            "task intervals",
            "local setup",
            "cross setup",
        ] {
            let output = support::run_test("teardown::teardown", mode);

            assert!(
                output.status.success(),
                "{mode}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        return;
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let count = Arc::new(AtomicUsize::new(0));
        let operation = operation::Operation {
            name: "primary",
            caller: std::panic::Location::caller(),
        };

        if mode.starts_with("local") {
            let (handle, mut owner) = local::channel::<(), ()>(NonZeroUsize::new(3).unwrap());

            if mode.ends_with("queue") {
                local::Request::new(
                    &handle,
                    operation,
                    async |_: &mut (), ()| panic!("primary operation"),
                    (),
                )
                .cast()
                .await
                .unwrap();

                for _ in 0..2 {
                    local::Request::new(
                        &handle,
                        operation,
                        async |_: &mut (), _: Broken| {},
                        Broken(count.clone()),
                    )
                    .cast()
                    .await
                    .unwrap();
                }
            } else if mode.ends_with("intervals") {
                for _ in 0..2 {
                    let capture = Broken(count.clone());
                    let callback: AktorClosure<dyn AktorIntervalLogic<()>> =
                        (async move |_: &mut ()| {
                            std::hint::black_box(&capture);
                        })
                        .into();

                    owner
                        .hooks
                        .intervals
                        .push(std::rc::Rc::new(std::cell::RefCell::new(Some(callback))));
                }

                drop(handle.shutdown());
            } else {
                let before = Broken(count.clone());
                let after = Broken(count.clone());

                owner.hooks.before_each = Some(Box::new(move |_, _| {
                    std::hint::black_box(&before);
                }));
                owner.hooks.after_each = Some(Box::new(move |_, _| {
                    std::hint::black_box(&after);
                }));
                drop(handle.shutdown());
            }

            let completion = owner.completion();

            if mode.ends_with("cancelled") {
                let _result = std::panic::catch_unwind(AssertUnwindSafe(|| drop(owner)));

                assert!(completion.wait().await.is_err());
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            if mode.ends_with("setup") {
                let result = owner
                    .run_with(async || Err(((), "primary setup".into())), async |_| Ok(()))
                    .await;

                assert!(result.unwrap_err().1.contains("primary setup"));
                let (_, report) = completion.wait().await.unwrap_err();
                assert!(report.contains("primary setup"));
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            let queued = mode.ends_with("queue");
            let dropped = count.clone();
            let payload = AssertUnwindSafe(owner.run((), async move |_| {
                if queued {
                    assert_eq!(dropped.load(Ordering::SeqCst), 2);
                }

                Ok(())
            }))
            .catch_unwind()
            .await
            .unwrap_err();

            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&if mode.ends_with("queue") {
                    "primary operation"
                } else {
                    "independent destructor"
                })
            );
            assert!(completion.wait().await.is_err());
        } else if mode.starts_with("cross") {
            let (handle, mut owner) = cross_core::channel::<()>(NonZeroUsize::new(3).unwrap());

            if mode.ends_with("queue") {
                crash(&handle).cast().await.unwrap();

                for _ in 0..2 {
                    task_discard(&handle, Broken(count.clone()))
                        .cast()
                        .await
                        .unwrap();
                }
            } else if mode.ends_with("intervals") {
                let mut intervals = Vec::new();

                for _ in 0..2 {
                    let capture = Broken(count.clone());
                    let callback: AktorClosure<dyn AktorIntervalLogic<()>> =
                        (async move |_: &mut ()| {
                            std::hint::black_box(&capture);
                        })
                        .into();

                    intervals.push(AktorInterval::new(Duration::from_secs(60), callback).unwrap());
                }

                owner.set_intervals(intervals);
                drop(handle.shutdown());
            } else {
                let before = Broken(count.clone());
                let after = Broken(count.clone());

                owner.hooks.before_each = Some(Box::new(move |_, _| {
                    std::hint::black_box(&before);
                }));
                owner.hooks.after_each = Some(Box::new(move |_, _| {
                    std::hint::black_box(&after);
                }));
                drop(handle.shutdown());
            }

            if mode.ends_with("cancelled") {
                let _result = std::panic::catch_unwind(AssertUnwindSafe(|| drop(owner)));

                assert!(handle.completion().wait().await.is_err());
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            if mode.ends_with("setup") {
                let result = owner
                    .run_with(
                        async || Err((cross_core::OwnerError::Setup, "primary setup".into())),
                        async |_| Ok(()),
                    )
                    .await;

                assert!(result.unwrap_err().1.contains("primary setup"));
                let (_, report) = handle.completion().wait().await.unwrap_err();
                assert!(report.contains("primary setup"));
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            let queued = mode.ends_with("queue");
            let dropped = count.clone();
            let payload = AssertUnwindSafe(owner.run_with(
                async || Ok(()),
                async move |_| {
                    if queued {
                        assert_eq!(dropped.load(Ordering::SeqCst), 2);
                    }

                    Ok(())
                },
            ))
            .catch_unwind()
            .await
            .unwrap_err();

            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&if mode.ends_with("queue") {
                    "primary operation"
                } else {
                    "independent destructor"
                })
            );
            assert!(handle.completion().wait().await.is_err());
        } else {
            let before = (mode.ends_with("hooks") || mode.ends_with("before"))
                .then(|| Broken(count.clone()));
            let after = mode.ends_with("hooks").then(|| Broken(count.clone()));
            let panic_before = mode.ends_with("before");
            let mut setup = AktorNew {
                name: "teardown task".into(),
                kind: AktorKind::TokioTask,
                start: aktor::AktorInit::Ready(async || ()),
                end: (),
                hooks: AktorHooks {
                    before_each: move |_: &mut (), _: operation::Operation| {
                        std::hint::black_box(&before);
                        if panic_before {
                            panic!("primary hook");
                        }
                    },
                    after_each: move |_: &mut (), _: operation::Operation| {
                        std::hint::black_box(&after);
                    },
                    intervals: Vec::new(),
                },
                capacity: NonZeroUsize::new(3).unwrap(),
                stuck: AktorStuck::Kill,
            };

            if mode.ends_with("intervals") {
                for _ in 0..2 {
                    let capture = Broken(count.clone());

                    setup.hooks.intervals.push(
                        AktorInterval::new(Duration::from_secs(60), move |_: AktorTaskState<()>| {
                            std::hint::black_box(&capture);
                            async {}
                        })
                        .unwrap(),
                    );
                }
            }

            let actors = aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: setup,
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            actors.ready().await.unwrap();
            let latest = if mode == "task latest before" {
                Some(task_discard(actors.handles(), Broken(count.clone())).latest())
            } else {
                None
            };

            if mode == "task before" {
                task_discard(actors.handles(), Broken(count.clone()))
                    .cast()
                    .await
                    .unwrap();
            } else if mode.ends_with("queue") {
                let (entered, entering) = tokio::sync::oneshot::channel();
                let (release, released) = tokio::sync::oneshot::channel();

                task_crash(actors.handles(), entered, released)
                    .cast()
                    .await
                    .unwrap();
                entering.await.unwrap();

                for _ in 0..2 {
                    task_discard(actors.handles(), Broken(count.clone()))
                        .cast()
                        .await
                        .unwrap();
                }

                release.send(()).unwrap();
            } else if latest.is_none() {
                actors.killswitch().stop();
            }

            let report = actors.completion().await.unwrap();

            assert!(report.failed(), "{report}");
            assert_eq!(
                report.to_string().matches("independent destructor").count(),
                2,
                "{report}"
            );

            if mode.ends_with("queue") || mode.ends_with("before") {
                assert!(
                    report.failures[0]
                        .error
                        .contains(if mode.ends_with("queue") {
                            "primary operation"
                        } else {
                            "primary hook"
                        })
                );
            }

            drop(latest);
        }

        assert_eq!(count.load(Ordering::SeqCst), 2);
    });
}

#[aktor]
async fn task_crash(
    _: &mut (),
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
) {
    entered.send(()).unwrap();
    release.await.unwrap();
    panic!("primary operation");
}
#[aktor(latest)]
async fn task_discard(_: &mut (), _: Broken) {}

#[aktor]
async fn crash(_: &mut ()) {
    panic!("primary operation");
}
