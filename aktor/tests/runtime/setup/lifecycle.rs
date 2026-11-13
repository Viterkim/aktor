use super::*;
use std::time::Duration;

#[cfg(feature = "embassy_cross_core")]
#[tokio::test]
async fn cleanup_primary() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    struct Hook;
    impl Drop for Hook {
        fn drop(&mut self) {
            panic!("hook drop died");
        }
    }

    let (handle, mut owner) = cross_core::channel::<u32>(1).unwrap();
    let hook = Hook;

    owner.hooks.before_each = Some(Box::new(move |_, _| {
        std::hint::black_box(&hook);
    }));
    drop(handle.shutdown());

    let result = AssertUnwindSafe(owner.run_with(
        async || Ok(7),
        async |_| {
            panic!("cleanup died");
            #[allow(unreachable_code)]
            Ok(())
        },
    ))
    .catch_unwind()
    .await
    .unwrap_err();

    assert_eq!(result.downcast_ref::<&str>(), Some(&"cleanup died"));
    assert!(handle.completion().wait().await.is_err());
}

#[cfg(feature = "local")]
struct QueuedDrop;
#[cfg(feature = "local")]
impl Drop for QueuedDrop {
    fn drop(&mut self) {
        panic!("queued input drop died");
    }
}

#[cfg(feature = "local")]
#[tokio::test]
async fn queued_drop_primary() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let (handle, owner) = local::channel::<u32, 2, ()>().unwrap();
    let operation = operation::Operation {
        name: "panic",
        caller: std::panic::Location::caller(),
    };

    local::Request::new(
        &handle,
        operation,
        async |_: &mut u32, ()| panic!("operation died"),
        (),
    )
    .cast()
    .await;

    local::Request::new(
        &handle,
        operation,
        async |_: &mut u32, _: QueuedDrop| {},
        QueuedDrop,
    )
    .cast()
    .await;

    let cleaned = Rc::new(Cell::new(0));
    let cleanup = cleaned.clone();
    let completion = owner.completion();
    let payload = AssertUnwindSafe(owner.run(7, async move |_| {
        cleanup.set(cleanup.get() + 1);
        Ok(())
    }))
    .catch_unwind()
    .await
    .unwrap_err();

    assert_eq!(payload.downcast_ref::<&str>(), Some(&"operation died"));
    assert_eq!(cleaned.get(), 1);
    assert!(completion.wait().await.is_err());
    #[cfg(feature = "embassy_cross_core")]
    {
        #[aktor]
        async fn crash(_: &mut u32) {
            panic!("operation died");
        }
        #[aktor]
        async fn discard(_: &mut u32, _: QueuedDrop) {}

        let (handle, owner) = cross_core::channel::<u32>(2).unwrap();

        crash(&handle).cast().await;
        discard(&handle, QueuedDrop).cast().await;

        let payload = AssertUnwindSafe(owner.run_with(async || Ok(7), async |_| Ok(())))
            .catch_unwind()
            .await
            .unwrap_err();

        assert_eq!(payload.downcast_ref::<&str>(), Some(&"operation died"));
        assert!(handle.completion().wait().await.is_err());
    }
}

#[cfg(feature = "std_thread")]
#[tokio::test]
async fn data() {
    use er::{Er, ErErrorPresentationExt, ErPresentationExt};

    let mut group = AktorGroup::new();

    group.start().unwrap();

    let handle = aktor_start_in(
        &group,
        AktorNew {
            name: "typed counter".into(),
            kind: AktorKind::TokioThread,
            start: async || Ok::<_, String>(17_u32),
            end: async |_: u32| Err(String::from("cleanup data")),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
    )
    .await
    .unwrap();

    #[derive(Er)]
    #[er(format = "Close counter {value}")]
    struct CloseErr {
        value: u32,
    }

    let tree = aktor_start_in(
        &group,
        AktorNew {
            name: "ordinary cleanup".into(),
            kind: AktorKind::StdThread,
            start: async || Ok::<_, String>(31_u32),
            end: async |value| {
                Err(er::ErTree::new(
                    CloseErr { value },
                    [
                        std::io::Error::other("flush failed"),
                        std::io::Error::other("journal failed"),
                    ],
                ))
                .er_report()
            },
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
    )
    .await
    .unwrap();

    let (error, _) = handle.shutdown().wait().await.unwrap_err();

    match &*error {
        owner::OwnerError::Cleanup(errors) => {
            assert_eq!(errors.errors[0].diagnostics, "cleanup data");
        }
        error => panic!("unexpected completion: {error:?}"),
    }

    let (error, report) = tree.shutdown().wait().await.unwrap_err();

    let owner::OwnerError::Cleanup(errors) = &*error else {
        panic!("cleanup tree was lost: {error}");
    };
    let diagnostic = &report;

    assert!(diagnostic.contains("journal failed"));
    assert_eq!(
        errors.errors[0].diagnostics.matches("flush failed").count(),
        1
    );

    assert!(group.shutdown().await.failed());

    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = cleaned.clone();
    let result = aktor_start(AktorSetup {
        actors: (
            AktorNew {
                name: "ready".into(),
                kind: AktorKind::TokioThread,
                start: async || Ok::<_, String>(0_u32),
                end: async move |_: u32| {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(String::from("rollback cleanup refused"))
                },
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "failed".into(),
                kind: AktorKind::StdThread,
                start: async || Err::<u32, _>(String::from("setup data")),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await;

    let (failure, printed) = result.err().unwrap();
    assert_eq!(failure.to_string(), "actor startup failed");
    assert_eq!(printed.matches("setup data").count(), 1);
    assert_eq!(printed.matches("rollback cleanup refused").count(), 1);
    assert!(!failure.er_report().contains("rollback cleanup refused"));
    let tree = er::ErTree::from(failure);
    match tree.top {
        AktorStartError {
            cause: AktorStartCause::Startup(error),
            report: Some(report),
            ..
        } if matches!(
            error.cause.primary(),
            aktor::listener::DedicatedStartError::Init(_)
        ) =>
        {
            assert!(!report.ran_past_grace, "{report}");
            assert_eq!(
                report.to_string().matches("setup data").count(),
                1,
                "{report}"
            );

            let ready = report
                .actors
                .iter()
                .find(|actor| actor.actor == "ready")
                .unwrap();

            assert!(
                ready.diagnostics[0]
                    .diagnostics
                    .contains("rollback cleanup refused")
            );
        }
        _ => panic!("typed setup failure was lost"),
    }

    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[cfg(feature = "local")]
#[tokio::test]
async fn diagnostic() {
    use super::support::poll;

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            for rollback in [true, false] {
                let startup = aktor_start(AktorSetup {
                    actors: (
                        AktorNew {
                            name: "cleanup only".into(),
                            kind: AktorKind::TokioLocal(&executor),
                            start: async || Ok::<_, String>(()),
                            end: async |_: ()| Err(String::from("cleanup refused")),
                            hooks: Default::default(),
                            capacity: 32,
                            stuck: AktorStuck::Kill,
                        },
                        AktorNew {
                            name: "sibling".into(),
                            kind: AktorKind::TokioLocal(&executor),
                            start: async move || {
                                if rollback {
                                    Err::<(), _>(String::from("setup refused"))
                                } else {
                                    Ok(())
                                }
                            },
                            end: (),
                            hooks: Default::default(),
                            capacity: 32,
                            stuck: AktorStuck::Kill,
                        },
                    ),
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                })
                .await;

                let report = if rollback {
                    *startup.err().unwrap().0.report.unwrap()
                } else {
                    startup.unwrap().shutdown().await
                };
                let cleanup = report
                    .actors
                    .iter()
                    .find(|actor| actor.actor == "cleanup only")
                    .unwrap();
                let diagnostics = &cleanup.diagnostics;

                assert!(report.failed());
                assert_eq!(diagnostics.len(), usize::from(rollback), "{report}");
                assert_eq!(report.to_string().matches("cleanup refused").count(), 1);

                if rollback {
                    assert_eq!(diagnostics[0].diagnostics, "cleanup refused");
                } else {
                    assert!(
                        report
                            .failure
                            .as_ref()
                            .unwrap()
                            .message
                            .contains("cleanup refused")
                    );
                }
            }
        })
        .await;

    let mut group = local::AktorGroup::<local::clock::Tokio>::new();
    let mut driver = group.listen().unwrap();
    let _handle = group
        .spawn_args::<(), 1, ()>(AktorArgs {
            name: "driven cleanup only".into(),
            capacity: 1,
            setup: async || Ok(()),
            cleanup: async |_| Err(AktorError::new("cleanup refused", ())),
        })
        .unwrap();

    assert!(poll(&mut driver).is_pending());
    group.killswitch().stop();

    let report = driver.await;

    assert!(report.failed());
    assert_eq!(report.actors[0].diagnostics.len(), 1, "{report}");
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local_data() {
    use er::ErErrorPresentationExt;
    use std::error::Error;

    struct Data(Rc<Cell<u32>>);

    fn assert_reportable<E: Error + Send + Sync + 'static>() {}
    assert_reportable::<local::OwnerError>();

    async fn ready<E>(
        handle: &local::Handle<(), 1, E>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        handle.ready().await.map_err(|(error, _)| error)?;
        Ok(())
    }

    async fn completion<E>(completion: &local::Completion<E>) -> er::ErTest {
        er::ErTestExt::er_test(completion.await)?;
        Ok(())
    }

    async fn owned_completion<E: 'static>(completion: local::Completion<E>) -> er::ErTest {
        er::ErTestExt::er_test(completion.await)?;
        Ok(())
    }

    for setup_failure in [true, false] {
        let (handle, owner) = local::channel::<(), 1, Data>().unwrap();
        let completed = owner.completion();
        let value = Rc::new(Cell::new(17));
        let data = Data(value.clone());
        handle.shutdown();

        let result = if setup_failure {
            owner
                .run_with(
                    async move || {
                        Err(AktorError {
                            diagnostics: "local data".into(),
                            data,
                        })
                    },
                    async |_| Ok(()),
                )
                .await
        } else {
            owner
                .run((), async move |_| {
                    Err(AktorError {
                        diagnostics: "local data".into(),
                        data,
                    })
                })
                .await
        };

        let (error, report) = result.unwrap_err();
        let error: Box<dyn Error + Send + Sync> = error.into();
        assert!(!error.er_report().contains("local data"));
        assert!(completion(&completed).await.is_err());
        assert!(owned_completion(completed.clone()).await.is_err());

        let (typed, _) = completed.wait_with_data().await.unwrap_err();
        let (observed, _) = completed.clone().wait_with_data().await.unwrap_err();
        assert!(Rc::ptr_eq(&typed.cause, &observed.cause));
        let local_report: Box<dyn Error> = typed.clone().into();
        assert!(!local_report.er_report().contains("local data"));
        let data = match &*typed.cause {
            local::OwnerError::Setup(error) | local::OwnerError::Cleanup(error) => &error.data,
            error => panic!("unexpected lifecycle error: {error}"),
        };
        assert!(Rc::ptr_eq(&data.0, &value));

        if setup_failure {
            assert!(ready(&handle).await.is_err());
            let (ready, _) = handle.ready_with_data().await.unwrap_err();
            assert!(Rc::ptr_eq(&ready.cause, &typed.cause));
        } else {
            ready(&handle).await.unwrap();
        }

        drop((typed, observed, local_report, completed, handle));
        assert!(
            std::thread::spawn(move || report)
                .join()
                .unwrap()
                .contains("local data")
        );
    }

    let data = Cell::new(13);
    let (handle, owner) = local::channel::<(), 1, &Cell<u32>>().unwrap();
    let completed = owner.completion();
    let (_, report) = owner
        .run_with(
            async || Err(AktorError::new("borrowed setup data", &data)),
            async |_| Ok(()),
        )
        .await
        .unwrap_err();

    assert!(ready(&handle).await.is_err());
    let (typed, _) = completed.wait_with_data().await.unwrap_err();
    match &*typed.cause {
        local::OwnerError::Setup(error) => assert!(std::ptr::eq(error.data, &data)),
        error => panic!("unexpected lifecycle error: {error}"),
    }

    drop((typed, completed, handle));
    assert!(
        std::thread::spawn(move || report)
            .join()
            .unwrap()
            .contains("borrowed setup data")
    );
}

#[cfg(feature = "local")]
#[tokio::test]
async fn initialization() {
    use er::ErErrorContextExt;
    use futures_util::FutureExt;
    use std::{cell::Cell, panic::AssertUnwindSafe, rc::Rc};

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let count = cleaned.clone();
            let (failed, failed_report) = aktor_start(AktorSetup {
                actors: (
                    AktorNew {
                        name: "ready sibling".into(),
                        kind: AktorKind::TokioLocal(&executor),
                        start: async || Ok::<_, String>(()),
                        end: async move |_| {
                            tokio::task::yield_now().await;
                            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Ok::<_, String>(())
                        },
                        hooks: Default::default(),
                        capacity: 32,
                        stuck: AktorStuck::Kill,
                    },
                    AktorNew {
                        name: "panicked setup".into(),
                        kind: AktorKind::TokioLocal(&executor),
                        start: async || -> Result<(), String> {
                            panic!("local initialization sentinel")
                        },
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
            assert_eq!(
                failed_report
                    .matches("local initialization sentinel")
                    .count(),
                1,
                "{failed_report}"
            );
            assert!(!failed_report.contains("before cleanup completed"));

            let wrapped = (failed, failed_report).er(|| std::fmt::Error);
            assert_eq!(
                wrapped
                    .er_report()
                    .matches("local initialization sentinel")
                    .count(),
                1
            );
        })
        .await;

    struct Released(Rc<Cell<usize>>, local::Completion<String>);
    impl Drop for Released {
        fn drop(&mut self) {
            assert!((&self.1).into_future().now_or_never().is_none());
            self.0.set(self.0.get() + 1);
        }
    }

    let (handle, mut owner) = local::channel::<(), 1, String>().unwrap();
    let completion = owner.completion();
    let disposed = Rc::new(Cell::new(0));
    let hook = Released(disposed.clone(), owner.completion());
    let cleanup = Released(disposed.clone(), owner.completion());

    owner.hooks.before_each = Some(Box::new(move |_, _| {
        std::hint::black_box(&hook);
    }));
    let result = AssertUnwindSafe(owner.run_with(
        async || -> Result<(), AktorError<String>> { panic!("retained initialization sentinel") },
        async move |_| {
            std::hint::black_box(&cleanup);
            Ok(())
        },
    ))
    .catch_unwind()
    .await;

    assert!(result.is_err());
    let (ready, _) = handle.ready_with_data().await.unwrap_err();
    let (completed, _) = completion.wait_with_data().await.unwrap_err();

    assert_eq!(disposed.get(), 2);
    assert!(Rc::ptr_eq(&ready.cause, &completed.cause));
    assert!(matches!(&*ready.cause, local::OwnerError::SetupPanic(error)
        if error.diagnostics == "retained initialization sentinel"));

    let (handle, owner) = local::channel::<(), 1, String>().unwrap();
    let completion = owner.completion();

    drop(owner);
    assert!(matches!(
        handle.ready().await,
        Err((local::OwnerError::Cancelled, _))
    ));
    assert!(matches!(
        completion.wait().await,
        Err((local::OwnerError::Cancelled, _))
    ));
}
