#![cfg(all(feature = "tokio", feature = "local"))]

use aktor::{
    ActorArgs, AktorClosures, AktorError, AktorKind, AktorName, AktorNew, AktorNewOptions,
    AktorNoRole, AktorOptions, AktorSetup, aktor_start, local, operation::Operation,
};
use futures_util::FutureExt;
use std::{cell::Cell, panic::AssertUnwindSafe, rc::Rc, time::Duration};

struct DropFailure {
    dropped: Rc<Cell<bool>>,
    fails: bool,
}
impl Drop for DropFailure {
    fn drop(&mut self) {
        self.dropped.set(true);

        if self.fails {
            panic!("application destructor failed");
        }
    }
}

#[tokio::test]
async fn shutdown() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for fault in 0..4 {
                let cleaned = Rc::new(Cell::new(0));
                let clean = cleaned.clone();
                let checked = cleaned.clone();
                let hooks = Rc::new(Cell::new(0));
                let called = hooks.clone();
                let released = Rc::new(Cell::new(false));
                let release = released.clone();
                let held = (fault == 3).then(|| DropFailure {
                    dropped: Rc::new(Cell::new(false)),
                    fails: true,
                });
                let actors = aktor_start(AktorSetup {
                    actors: AktorNew {
                        name: AktorName::new("local shutdown hook"),
                        role: AktorNoRole,
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok(())
                        }),
                        closures: AktorClosures {
                            start: async || Ok(()),
                            end: Some(
                                (async move |_| {
                                    core::future::poll_fn(|cx| {
                                        if release.get() {
                                            core::task::Poll::Ready(())
                                        } else {
                                            cx.waker().wake_by_ref();
                                            core::task::Poll::Pending
                                        }
                                    })
                                    .await;
                                    tokio::task::yield_now().await;
                                    clean.set(clean.get() + 1);
                                    Ok(())
                                })
                                .into(),
                            ),

                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: AktorNewOptions { capacity: 32 },
                    },
                    shutdown: move |report| {
                        called.set(called.get() + 1);
                        assert_eq!(checked.get(), 1);
                        assert_eq!(report.actors.len(), 1);
                        drop(held);
                    },
                    options: AktorOptions {
                        shutdown_grace: Duration::from_secs(5),
                    },
                })
                .await
                .unwrap();

                let (entered, running) = tokio::sync::oneshot::channel();

                actors
                    .group
                    .spawn_task(async move {
                        let captured = DropFailure {
                            dropped: released,
                            fails: false,
                        };
                        entered.send(()).unwrap();
                        core::future::pending::<()>().await;
                        drop(captured);
                    })
                    .unwrap();

                running.await.unwrap();

                let completion = actors.completion();
                let operation = Operation {
                    name: "local query",
                    caller: std::panic::Location::caller(),
                };
                let domain = local::Request::new(
                    &actors.handles,
                    operation,
                    async |_: &mut (), ()| Err::<(), _>("query refused"),
                    (),
                )
                .await;

                assert_eq!(domain, Err("query refused"));
                assert!(!actors.killswitch().is_stopping());

                if fault == 1 || fault == 2 {
                    let request = local::Request::new(
                        &actors.handles,
                        operation,
                        async |_: &mut (), ()| panic!("local owner failed"),
                        (),
                    );

                    if fault == 1 {
                        drop(request.send().await);
                    } else {
                        request.cast().await;
                    }

                    actors.killswitch().wait_stopping().await;
                } else {
                    actors.killswitch().stop();
                }

                let report = tokio::time::timeout(Duration::from_secs(2), completion.wait())
                    .await
                    .expect("shutdown hook stalled");

                assert_eq!(hooks.get(), 1);
                assert_eq!(cleaned.get(), 1);
                assert!(!report.timed_out, "{report}");
                assert_eq!(report.failed(), fault != 0, "{report}");

                if fault == 1 || fault == 2 {
                    assert_eq!(report.failure.unwrap().message, "local owner failed");
                } else if fault == 3 {
                    assert!(report.to_string().contains("application destructor failed"));
                }

                assert_eq!(actors.shutdown().await.actors.len(), 1);
                assert_eq!(hooks.get(), 1);
            }

            let dropped = Rc::new(Cell::new(false));
            let held = DropFailure {
                dropped: dropped.clone(),
                fails: false,
            };
            let calls = Rc::new(Cell::new(0));
            let called = calls.clone();
            let startup = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: AktorName::new("unpolled local hook"),
                    role: AktorNoRole,
                    kind: AktorKind::Local::<local::clock::Tokio>(|_| {
                        panic!("unpolled startup installed a driver");
                    }),
                    closures: AktorClosures {
                        start: async || Ok(()),
                        end: None,
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: AktorNewOptions { capacity: 32 },
                },
                shutdown: move |_| {
                    called.set(called.get() + 1);
                    drop(held);
                },
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
            });
            let completion = startup.completion();
            drop(startup);
            assert!(dropped.get(), "unpolled hook retained its captures");
            assert_eq!(calls.get(), 0);
            assert!(completion.try_report().is_some());
        })
        .await;
}

#[tokio::test]
async fn panic_cleanup() {
    for mode in 0..3 {
        let group = local::AktorGroup::<local::clock::Tokio>::new();
        let completion = group.completion();
        let cleaned = Rc::new(Cell::new(0));
        let clean = cleaned.clone();
        let notified = Rc::new(Cell::new(false));
        let notification = notified.clone();
        let cleanup_done = cleaned.clone();
        group
            .on_shutdown(move |_| {
                assert_eq!(cleanup_done.get(), 1);
                notification.set(true);
            })
            .unwrap();
        let dropped = Rc::new(Cell::new(false));
        let drop_signal = dropped.clone();
        let hook_calls = Rc::new(Cell::new(0));
        let hooks = hook_calls.clone();

        let run = group.run(
            async move |group| {
                let handle = group
                    .spawn::<(), 1, ()>(ActorArgs {
                        name: "application actor".into(),
                        capacity: 1,
                        setup: async || Ok(()),
                        cleanup: async move |_| {
                            assert!(mode == 0 || drop_signal.get());
                            tokio::task::yield_now().await;
                            clean.set(clean.get() + 1);
                            Ok(())
                        },
                    })
                    .unwrap();

                handle.ready().await.unwrap();

                if mode == 0 {
                    panic!("application poll failed");
                }

                let captured = DropFailure {
                    dropped,
                    fails: true,
                };

                if mode == 1 {
                    return Err(AktorError {
                        diagnostics: "application domain failure".into(),
                        data: captured,
                    });
                }

                group.killswitch().stop();
                core::future::pending::<()>().await;
                drop(captured);
                Ok(())
            },
            move |_| hooks.set(hooks.get() + 1),
        );
        let result =
            tokio::time::timeout(Duration::from_secs(2), AssertUnwindSafe(run).catch_unwind())
                .await
                .expect("local cleanup stalled")
                .expect("application panic escaped its group");
        let report = result.expect_err("application failure was lost");

        assert_eq!(cleaned.get(), 1, "{report}");
        assert!(notified.get());
        assert_eq!(hook_calls.get(), 1);
        assert!(!report.timed_out, "{report}");
        assert_eq!(report.actors.len(), 1, "{report}");
        assert!(report.actors[0].diagnostics.is_empty(), "{report}");
        assert!(completion.try_report().unwrap().failed());

        if mode == 0 {
            assert_eq!(
                report.failure.as_ref().unwrap().message,
                "application poll failed"
            );
        } else {
            assert!(report.to_string().contains("application destructor failed"));

            if mode == 1 {
                assert!(report.to_string().contains("application domain failure"));
            }
        }
    }
}
