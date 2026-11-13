#![cfg(all(feature = "tokio", feature = "local"))]

use aktor::{
    AktorKind, AktorNew, AktorSetup, AktorStuck, aktor_start, local, operation::Operation,
};
use std::{cell::Cell, rc::Rc, time::Duration};

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
            for fault in 0..6 {
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
                        name: "local shutdown hook".into(),
                        kind: AktorKind::Local::<local::clock::Tokio>(|future| {
                            tokio::task::spawn_local(future);
                            Ok(())
                        }),
                        start: async || aktor::aktor_ready(()),
                        end: async move |_| {
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

                            if fault >= 4 {
                                Err(std::io::Error::other("local cleanup sentinel"))
                            } else {
                                Ok(())
                            }
                        },
                        hooks: Default::default(),
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: move |_| {
                        called.set(called.get() + 1);
                        assert_eq!(checked.get(), 1);
                        drop(held);
                    },
                    grace: Duration::from_secs(5),
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

                if fault == 1 || fault == 2 || fault == 5 {
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
                assert!(!report.ran_past_grace, "{report}");
                assert_eq!(report.failed(), fault != 0, "{report}");

                if fault >= 4 {
                    assert_eq!(
                        report.to_string().matches("local cleanup sentinel").count(),
                        1
                    );
                    if fault == 4 {
                        let completion = actors.handles.completion();
                        let (error, diagnostic) = completion.wait().await.unwrap_err();

                        assert_eq!(error.to_string(), "actor cleanup failed");
                        assert_eq!(diagnostic.matches("local cleanup sentinel").count(), 1);

                        let (_, detail) = completion.wait_with_data().await.unwrap_err();
                        assert_eq!(detail, diagnostic);
                    }
                }

                if fault == 1 || fault == 2 || fault == 5 {
                    assert_eq!(
                        report.failure.as_ref().unwrap().message,
                        "local owner failed"
                    );
                } else if fault == 3 {
                    assert!(report.to_string().contains("application destructor failed"));
                }

                actors.shutdown().await;
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
                    name: "unpolled local hook".into(),
                    kind: AktorKind::Local::<local::clock::Tokio>(|_| {
                        panic!("unpolled startup installed a driver");
                    }),
                    start: async || aktor::aktor_ready(()),
                    end: (),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                shutdown: move |_| {
                    called.set(called.get() + 1);
                    drop(held);
                },
                grace: Duration::from_secs(5),
            });
            let completion = startup.completion();
            drop(startup);
            assert!(dropped.get(), "unpolled hook retained its captures");
            assert_eq!(calls.get(), 0);
            assert!(completion.try_report().is_some());
        })
        .await;
}

#[tokio::test(start_paused = true)]
async fn drop_before_next_end() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (held, released) = tokio::sync::oneshot::channel::<()>();
            let kind = || {
                AktorKind::Local::<local::clock::Tokio>(|future| {
                    tokio::task::spawn_local(future);
                    Ok(())
                })
            };

            let reader = AktorNew {
                name: "reader".into(),
                kind: kind(),
                start: async move || aktor::aktor_ready(released),
                end: async |released: tokio::sync::oneshot::Receiver<()>| {
                    let _ = released.await;
                },
                hooks: Default::default(),
                capacity: 1,
                stuck: AktorStuck::Leave,
            };
            let writer = AktorNew {
                name: "writer".into(),
                kind: kind(),
                start: async move || aktor::aktor_ready(held),
                end: async |held: tokio::sync::oneshot::Sender<()>| {
                    core::future::pending::<()>().await;
                    drop(held);
                },
                hooks: Default::default(),
                capacity: 1,
                stuck: AktorStuck::Leave,
            };

            let actors = aktor_start(AktorSetup {
                actors: (reader, writer),
                shutdown: |_| {},
                grace: Duration::from_secs(1),
            })
            .await
            .unwrap();
            let report = actors.shutdown().await;

            let reader = report
                .actors
                .iter()
                .find(|actor| actor.actor == "reader")
                .unwrap();
            assert_eq!(
                reader.retirement,
                aktor::AktorRetireStatus::Confirmed,
                "{report}"
            );
        })
        .await;
}
