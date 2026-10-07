use super::*;
use aktor::setup::closures::AktorIntervalLogic;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Broken(Arc<AtomicUsize>);
impl Drop for Broken {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("independent destructor");
    }
}

#[test]
fn teardown() {
    let Ok(mode) = std::env::var("AKTOR_CHILD") else {
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
            let output = support::child("teardown::teardown", mode);

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
            let (handle, mut owner) = local::channel::<(), 3, ()>().unwrap();

            if mode.ends_with("queue") {
                local::Request::new(
                    &handle,
                    operation,
                    async |_: &mut (), ()| panic!("primary operation"),
                    (),
                )
                .cast()
                .await;

                for _ in 0..2 {
                    local::Request::new(
                        &handle,
                        operation,
                        async |_: &mut (), _: Broken| {},
                        Broken(count.clone()),
                    )
                    .cast()
                    .await;
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
                assert_eq!(completion.diagnostics().len(), 2);
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            if mode.ends_with("setup") {
                let result = owner
                    .run_with(
                        async || Err(AktorSetupError::new("primary setup")),
                        async |_| Ok(()),
                    )
                    .await;

                assert!(format!("{:#}", result.unwrap_err()).contains("primary setup"));
                assert_eq!(completion.diagnostics().len(), 2);
                let error = completion.wait().await.unwrap_err();
                assert!(format!("{error:#}").contains("primary setup"));
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            let payload = AssertUnwindSafe(owner.run((), async |_| Ok(())))
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
            assert_eq!(completion.diagnostics().len(), 2);
            assert!(completion.wait().await.is_err());
        } else if mode.starts_with("cross") {
            let (handle, mut owner) = cross_core::channel::<()>(3).unwrap();

            if mode.ends_with("queue") {
                crash(&handle).cast().await;

                for _ in 0..2 {
                    task_discard(&handle, Broken(count.clone())).cast().await;
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

                    intervals.push((Duration::from_secs(60), callback));
                }

                owner.set_intervals(intervals).unwrap();
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
                assert_eq!(handle.completion().diagnostics().len(), 2);
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            if mode.ends_with("setup") {
                let result = owner
                    .run_with(
                        async || Err(AktorSetupError::new("primary setup")),
                        async |_| Ok(()),
                    )
                    .await;

                assert!(format!("{:#}", result.unwrap_err()).contains("primary setup"));
                assert_eq!(handle.completion().diagnostics().len(), 2);
                let error = handle.completion().wait().await.unwrap_err();
                assert!(format!("{error:#}").contains("primary setup"));
                assert_eq!(count.load(Ordering::SeqCst), 2);
                return;
            }

            let payload = AssertUnwindSafe(owner.run_with(async || Ok(()), async |_| Ok(())))
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
            assert_eq!(handle.completion().diagnostics().len(), 2);
            assert!(handle.completion().wait().await.is_err());
        } else {
            let mut setup = AktorNew {
                name: AktorName::new("teardown task"),
                role: AktorNoRole,
                kind: AktorKind::TokioTask,
                closures: AktorClosures {
                    start: async || Ok(()),
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 3 },
            };

            if mode.ends_with("hooks") {
                let before = Broken(count.clone());
                let after = Broken(count.clone());

                setup.closures.before_each = Some(
                    (move |_: &mut (), _: operation::Operation| {
                        std::hint::black_box(&before);
                    })
                    .into(),
                );
                setup.closures.after_each = Some(
                    (move |_: &mut (), _: operation::Operation| {
                        std::hint::black_box(&after);
                    })
                    .into(),
                );
            }

            if mode.ends_with("before") {
                let capture = Broken(count.clone());

                setup.closures.before_each = Some(
                    (move |_: &mut (), _: operation::Operation| {
                        std::hint::black_box(&capture);
                        panic!("primary hook");
                    })
                    .into(),
                );
            }

            if mode.ends_with("intervals") {
                for _ in 0..2 {
                    let capture = Broken(count.clone());

                    setup.closures.intervals.push(AktorInterval {
                        every: Duration::from_secs(60),
                        run: (move |_: AktorTaskState<()>| {
                            std::hint::black_box(&capture);
                            async {}
                        })
                        .into(),
                    });
                }
            }

            let actors = aktor_start(AktorSetup {
                actors: setup,
                shutdown: |_| {},
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
            })
            .await
            .unwrap();
            let latest = if mode == "task latest before" {
                Some(task_discard(&actors.handles, Broken(count.clone())).latest())
            } else {
                None
            };

            if mode == "task before" {
                task_discard(&actors.handles, Broken(count.clone()))
                    .cast()
                    .await;
            } else if mode.ends_with("queue") {
                let (entered, entering) = tokio::sync::oneshot::channel();
                let (release, released) = tokio::sync::oneshot::channel();

                task_crash(&actors.handles, entered, released).cast().await;
                entering.await.unwrap();

                for _ in 0..2 {
                    task_discard(&actors.handles, Broken(count.clone()))
                        .cast()
                        .await;
                }

                release.send(()).unwrap();
            } else if latest.is_none() {
                drop(actors.shutdown());
            }

            let report = actors.completion().await;

            assert!(report.failed(), "{report}");
            assert_eq!(report.actors[0].diagnostics.len(), 2, "{report}");

            if mode.ends_with("queue") || mode.ends_with("before") {
                assert!(
                    report
                        .failure
                        .unwrap()
                        .message
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
#[aktor]
async fn task_discard(_: &mut (), _: Broken) {}

#[aktor]
async fn crash(_: &mut ()) {
    panic!("primary operation");
}
