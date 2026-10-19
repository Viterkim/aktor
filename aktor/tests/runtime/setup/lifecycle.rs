use super::*;

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
            name: AktorName::new("typed counter"),
            role: aktors::Users,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async || Ok::<_, AktorSetupError>(17_u32),
                end: async |_: u32| Err(AktorCleanupError::new("cleanup data", ())),
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
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
            name: AktorName::new("ordinary cleanup"),
            role: AktorNoRole,
            kind: AktorKind::StdThread,
            closures: AktorClosures {
                start: async || Ok::<_, AktorSetupError>(31_u32),
                end: async |value| {
                    Err(er::ErTree::new(
                        CloseErr { value },
                        [
                            std::io::Error::other("flush failed"),
                            std::io::Error::other("journal failed"),
                        ],
                    ))
                    .er_into(|report| report)
                },
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: Default::default(),
        },
    )
    .await
    .unwrap();

    let error = handle.shutdown().wait().await.unwrap_err();

    match &error.error {
        owner::OwnerFailure::Cleanup(errors) => {
            assert_eq!(errors.errors[0].diagnostics, "cleanup data");
        }
        error => panic!("unexpected completion: {error:?}"),
    }

    let error = tree.shutdown().wait().await.unwrap_err();

    let owner::OwnerFailure::Cleanup(errors) = &error.error else {
        panic!("cleanup tree was lost: {error}");
    };
    let diagnostic = &errors.errors[0].diagnostics;

    assert!(diagnostic.contains("journal failed"));
    assert_eq!(
        errors.errors[0].diagnostics.matches("flush failed").count(),
        1
    );

    assert!(group.shutdown().await.failed());

    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = cleaned.clone();
    let result = aktor_start(AktorSetup {
        actors: aktor_setups! {
                    ready: AktorNew {

                        name: AktorName::new("ready"),
                        role: AktorNoRole,
                        kind: AktorKind::TokioThread,
                        closures: AktorClosures {
        start: async || Ok::<_, AktorSetupError>(0_u32),
                            end: async move |_: u32| {
                                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                Err(AktorCleanupError::new("rollback cleanup refused", ()))
                            },
                             intervals: vec![], before_each: None, after_each: None,
                        },
                        options: AktorNewOptions { capacity: 32 },
                    },
                    failed: AktorNew {

                        name: AktorName::new("failed"),
                        role: AktorNoRole,
                        kind: AktorKind::StdThread,
                        closures: AktorClosures {
        start: async || Err::<u32, _>(AktorSetupError {
                        diagnostics: "setup data".into(),
                        data: (),
                    }),  end: (), intervals: vec![], before_each: None, after_each: None },
                        options: AktorNewOptions { capacity: 32 },
                    },
                },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await;

    let failure = result.as_ref().err().unwrap();
    assert_eq!(failure.to_string(), "actor startup failed");
    let printed = &failure.diagnostics.report;
    assert_eq!(printed.matches("setup data").count(), 1);
    assert_eq!(printed.matches("rollback cleanup refused").count(), 1);
    assert!(
        !failure
            .er_report_string()
            .contains("rollback cleanup refused")
    );
    let tree = er::ErTree::from(result.err().unwrap());
    match tree.top {
        AktorStartupError {
            error: AktorStartError::Init(_),
            report: Some(report),
            ..
        } => {
            assert!(!report.timed_out, "{report}");
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
                            name: AktorName::new("cleanup only"),
                            role: AktorNoRole,
                            kind: AktorKind::TokioLocal(&executor),
                            closures: AktorClosures {
                                start: async || Ok::<_, AktorSetupError>(()),
                                end: async |_: ()| {
                                    Err(AktorCleanupError::new("cleanup refused", ()))
                                },
                                intervals: vec![],
                                before_each: None,
                                after_each: None,
                            },
                            options: AktorNewOptions { capacity: 32 },
                        },
                        AktorNew {
                            name: AktorName::new("sibling"),
                            role: AktorNoRole,
                            kind: AktorKind::TokioLocal(&executor),
                            closures: AktorClosures {
                                start: async move || {
                                    if rollback {
                                        Err::<(), _>(AktorSetupError::new("setup refused", ()))
                                    } else {
                                        Ok(())
                                    }
                                },
                                end: (),
                                intervals: vec![],
                                before_each: None,
                                after_each: None,
                            },
                            options: Default::default(),
                        },
                    ),
                    shutdown: |_| {},
                    options: AktorOptions {
                        shutdown_grace: Duration::from_secs(5),
                    },
                })
                .await;

                let report = if rollback {
                    *startup.err().unwrap().report.unwrap()
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
        .spawn::<(), 1, ()>(ActorArgs {
            name: "driven cleanup only".into(),
            capacity: 1,
            setup: async || Ok(()),
            cleanup: async |_| Err(AktorCleanupError::new("cleanup refused", ())),
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
        handle.ready().await?;
        Ok(())
    }

    async fn completion<E>(completion: &local::Completion<E>) -> er::ErTest {
        completion.await?;
        Ok(())
    }

    async fn owned_completion<E: 'static>(completion: local::Completion<E>) -> er::ErTest {
        completion.await?;
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
                        Err(AktorSetupError {
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
                    Err(AktorCleanupError {
                        diagnostics: "local data".into(),
                        data,
                    })
                })
                .await
        };

        let error: Box<dyn Error + Send + Sync> = result.unwrap_err().into();
        assert!(!error.er_report_string().contains("local data"));
        assert!(completion(&completed).await.is_err());
        assert!(owned_completion(completed.new_observer()).await.is_err());

        let typed = completed.wait_with_data().await.unwrap_err();
        let observed = completed.new_observer().wait_with_data().await.unwrap_err();
        assert!(Rc::ptr_eq(&typed.error, &observed.error));
        let local_report: Box<dyn Error> = typed.clone().into();
        assert!(!local_report.er_report_string().contains("local data"));
        let data = match &*typed.error {
            local::OwnerFailure::Setup(error) | local::OwnerFailure::Cleanup(error) => &error.data,
            error => panic!("unexpected lifecycle error: {error}"),
        };
        assert!(Rc::ptr_eq(&data.0, &value));

        if setup_failure {
            assert!(ready(&handle).await.is_err());
            let ready = handle.ready_with_data().await.unwrap_err();
            assert!(Rc::ptr_eq(&ready.error, &typed.error));
        } else {
            ready(&handle).await.unwrap();
        }

        drop((typed, observed, local_report, completed, handle));
        assert!(
            std::thread::spawn(move || format!("{error:#}"))
                .join()
                .unwrap()
                .contains("local data")
        );
    }

    let data = Cell::new(13);
    let (handle, owner) = local::channel::<(), 1, &Cell<u32>>().unwrap();
    let completed = owner.completion();
    let error = owner
        .run_with(
            async || Err(AktorSetupError::new("borrowed setup data", &data)),
            async |_| Ok(()),
        )
        .await
        .unwrap_err();

    assert!(ready(&handle).await.is_err());
    let typed = completed.wait_with_data().await.unwrap_err();
    match &*typed.error {
        local::OwnerFailure::Setup(error) => assert!(std::ptr::eq(error.data, &data)),
        error => panic!("unexpected lifecycle error: {error}"),
    }

    drop((typed, completed, handle));
    assert!(
        std::thread::spawn(move || format!("{error:#}"))
            .join()
            .unwrap()
            .contains("borrowed setup data")
    );
}

#[cfg(feature = "local")]
#[tokio::test]
async fn initialization() {
    use er::ErErrorPresentationExt;
    use futures_util::FutureExt;
    use std::{cell::Cell, panic::AssertUnwindSafe, rc::Rc};

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let count = cleaned.clone();
            let failed = aktor_start(AktorSetup {
                actors: (
                    AktorNew {
                        name: AktorName::new("ready sibling"),
                        role: AktorNoRole,
                        kind: AktorKind::TokioLocal(&executor),
                        closures: AktorClosures {
                            start: async || Ok::<_, AktorSetupError>(()),
                            end: async move |_| {
                                tokio::task::yield_now().await;
                                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                Ok::<_, AktorCleanupError>(())
                            },
                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: Default::default(),
                    },
                    AktorNew {
                        name: AktorName::new("panicked setup"),
                        role: AktorNoRole,
                        kind: AktorKind::TokioLocal(&executor),
                        closures: AktorClosures {
                            start: async || -> Result<(), AktorSetupError> {
                                panic!("local initialization sentinel")
                            },
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
            assert_eq!(
                format!("{failed:#}")
                    .matches("local initialization sentinel")
                    .count(),
                1,
                "{failed:#}"
            );
            assert_eq!(
                failed
                    .er_report_string()
                    .matches("local initialization sentinel")
                    .count(),
                0
            );
            assert!(!format!("{failed:#}").contains("before cleanup completed"));
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
        async || -> Result<(), AktorSetupError<String>> {
            panic!("retained initialization sentinel")
        },
        async move |_| {
            std::hint::black_box(&cleanup);
            Ok(())
        },
    ))
    .catch_unwind()
    .await;

    assert!(result.is_err());
    let ready = handle.ready_with_data().await.unwrap_err();
    let completed = completion.wait_with_data().await.unwrap_err();

    assert_eq!(disposed.get(), 2);
    assert!(Rc::ptr_eq(&ready.error, &completed.error));
    assert!(
        matches!(&*ready.error, local::OwnerFailure::SetupPanic(error)
        if error.diagnostics == "retained initialization sentinel")
    );

    let (handle, owner) = local::channel::<(), 1, String>().unwrap();
    let completion = owner.completion();

    drop(owner);
    assert!(matches!(
        handle.ready().await,
        Err(local::OwnerError {
            error: local::OwnerFailure::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        completion.wait().await,
        Err(local::OwnerError {
            error: local::OwnerFailure::Cancelled,
            ..
        })
    ));
}
