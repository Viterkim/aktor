use super::*;
use core::num::NonZeroUsize;
use er::*;
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

    let (handle, mut owner) = cross_core::channel::<u32>(NonZeroUsize::MIN);
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

    let (handle, owner) = local::channel::<u32, ()>(NonZeroUsize::new(2).unwrap());
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
    .await
    .unwrap();

    local::Request::new(
        &handle,
        operation,
        async |_: &mut u32, _: QueuedDrop| {},
        QueuedDrop,
    )
    .cast()
    .await
    .unwrap();

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

        let (handle, owner) = cross_core::channel::<u32>(NonZeroUsize::new(2).unwrap());

        crash(&handle).cast().await.unwrap();
        discard(&handle, QueuedDrop).cast().await.unwrap();

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
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "typed counter".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || 17_u32),
            end: async |_: u32| Err(((), String::from("cleanup data"))),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let group = started.group();
    let handle = started.handles();

    #[derive(Er)]
    #[er(format = "Close counter {value}")]
    struct CloseErr {
        value: u32,
    }

    let tree = group
        .spawn(AktorNew {
            name: "ordinary cleanup".into(),
            kind: AktorKind::StdThread,
            start: aktor::AktorInit::Ready(async || 31_u32),
            end: async |value| -> Result<(), (CloseErr, String)> {
                er_bail!(
                    CloseErr { value },
                    [
                        std::io::Error::other("flush failed"),
                        std::io::Error::other("journal failed"),
                    ],
                )
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        })
        .ready()
        .await
        .unwrap();

    let (error, message) = handle.shutdown().wait().await.unwrap_err();
    assert!(matches!(&*error, owner::OwnerError::Cleanup));
    assert!(message.contains("cleanup data"));

    let (error, report) = tree.shutdown().wait().await.unwrap_err();
    assert!(matches!(&*error, owner::OwnerError::Cleanup));
    assert!(report.contains("journal failed"));
    assert_eq!(report.matches("flush failed").count(), 1);

    assert!(group.stop().await.failed());

    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = cleaned.clone();
    let attempt = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: (
            AktorNew {
                name: "ready".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: async move |_: u32| {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(((), String::from("rollback cleanup refused")))
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "failed".into(),
                kind: AktorKind::StdThread,
                start: aktor::AktorInit::Try(async || {
                    Err::<u32, _>(((), String::from("setup data")))
                }),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    let result = attempt.ready().await;

    let (failure, printed) = result.err().unwrap();
    assert_eq!(printed.matches("setup data").count(), 1);
    assert_eq!(printed.matches("rollback cleanup refused").count(), 1);
    match failure {
        AktorStartError::Initializer { startup, .. } => {
            let report = startup.report.as_ref().unwrap();
            assert!(!report.ran_past_grace(), "{report}");
            assert_eq!(
                report.to_string().matches("setup data").count(),
                1,
                "{report}"
            );

            assert!(report.failures.iter().any(|failure| {
                matches!(&failure.source, aktor::AktorFailureSource::Actor { name, .. } if name == "ready")
                    && failure.error.contains("rollback cleanup refused")
            }));
        }
        _ => panic!("typed setup failure was lost"),
    }

    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[cfg(feature = "local")]
#[tokio::test]
async fn diagnostic() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            for rollback in [true, false] {
                let actors = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::KeepRunning,
                    actors: (
                        AktorNew {
                            name: "cleanup only".into(),
                            kind: AktorKind::TokioLocal(&executor),
                            start: aktor::AktorInit::Ready(async || ()),
                            end: async |_: ()| Err(((), String::from("cleanup refused"))),
                            hooks: AktorHooks::NONE,
                            capacity: NonZeroUsize::new(32).unwrap(),
                            stuck: AktorStuck::Kill,
                        },
                        AktorNew {
                            name: "sibling".into(),
                            kind: AktorKind::TokioLocal(&executor),
                            start: aktor::AktorInit::Try(async move || {
                                if rollback {
                                    Err::<(), _>(((), String::from("setup refused")))
                                } else {
                                    Ok(())
                                }
                            }),
                            end: (),
                            hooks: AktorHooks::NONE,
                            capacity: NonZeroUsize::new(32).unwrap(),
                            stuck: AktorStuck::Kill,
                        },
                    ),
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                });

                let report = match actors.ready().await {
                    Err((error, _)) => error.startup().unwrap().report.as_deref().unwrap().clone(),
                    Ok(_) => actors.stop().await,
                };
                let cleanup = report.failures.iter().filter(|failure| {
                    matches!(&failure.source, aktor::AktorFailureSource::Actor { name, phase: aktor::AktorPhase::End, .. } if name == "cleanup only")
                });

                assert_eq!(cleanup.count(), 1, "{report}");
                assert_eq!(report.to_string().matches("cleanup refused").count(), 1);
            }
        })
        .await;

    let executor = Rc::new(tokio::task::LocalSet::new());
    executor
        .run_until(async {
            let spawn = executor.clone();
            let started = aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "driven cleanup only".into(),
                    kind: AktorKind::Local::<local::clock::Tokio, _, _>(move |future| {
                        spawn.spawn_local(future);
                        Ok::<(), ((), String)>(())
                    }),
                    start: aktor::AktorInit::Ready(async || ()),
                    end: async |_| Err::<(), _>(((), String::from("cleanup refused"))),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::MIN,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            started.ready().await.unwrap();

            let report = started.stop().await;
            assert_eq!(
                report.to_string().matches("cleanup refused").count(),
                1,
                "{report}"
            );
        })
        .await;
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local_data() {
    use std::error::Error;

    struct Data(Rc<Cell<u32>>);

    fn assert_reportable<E: Error + Send + Sync + 'static>() {}
    assert_reportable::<local::OwnerError>();

    async fn ready<E>(handle: &local::Handle<(), E>) -> Result<(), Box<dyn Error + Send + Sync>> {
        handle.ready().await.map_err(|(error, _)| error)?;
        Ok(())
    }

    async fn completion<E>(completion: &local::Completion<E>) -> ErTest {
        ErTestExt::er_test(completion.await)?;
        Ok(())
    }

    async fn owned_completion<E: 'static>(completion: local::Completion<E>) -> ErTest {
        ErTestExt::er_test(completion.await)?;
        Ok(())
    }

    for setup_failure in [true, false] {
        let (handle, owner) = local::channel::<(), Data>(NonZeroUsize::MIN);
        let completed = owner.completion();
        let value = Rc::new(Cell::new(17));
        let data = Data(value.clone());
        handle.shutdown();

        let result = if setup_failure {
            owner
                .run_with(
                    async move || Err((data, "local data".into())),
                    async |_| Ok(()),
                )
                .await
        } else {
            owner
                .run((), async move |_| Err((data, "local data".into())))
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
            local::OwnerError::Setup(error) | local::OwnerError::Cleanup(error) => error,
            error => panic!("unexpected lifecycle error: {error}"),
        };
        assert!(Rc::ptr_eq(&data.0, &value));

        if setup_failure {
            assert!(ready(&handle).await.is_err());
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
    let (handle, owner) = local::channel::<(), &Cell<u32>>(NonZeroUsize::MIN);
    let completed = owner.completion();
    let (_, report) = owner
        .run_with(
            async || Err((&data, "borrowed setup data".into())),
            async |_| Ok(()),
        )
        .await
        .unwrap_err();

    assert!(ready(&handle).await.is_err());
    let (typed, _) = completed.wait_with_data().await.unwrap_err();
    match &*typed.cause {
        local::OwnerError::Setup(error) => assert!(std::ptr::eq(*error, &data)),
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
    use futures_util::FutureExt;
    use std::{cell::Cell, panic::AssertUnwindSafe, rc::Rc};

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let count = cleaned.clone();
            let attempt = aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::KeepRunning,
                actors: (
                    AktorNew {
                        name: "ready sibling".into(),
                        kind: AktorKind::TokioLocal(&executor),
                        start: aktor::AktorInit::Ready(async || ()),
                        end: async move |_| {
                            tokio::task::yield_now().await;
                            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        },
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    AktorNew {
                        name: "panicked setup".into(),
                        kind: AktorKind::TokioLocal(&executor),
                        start: aktor::AktorInit::Try(async || -> Result<(), ((), String)> {
                            panic!("local initialization sentinel")
                        }),
                        end: (),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                ),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            let (failed, failed_report) = attempt.ready().await.err().unwrap();

            assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                failed_report
                    .matches("local initialization sentinel")
                    .count(),
                1,
                "{failed_report}"
            );
            assert!(!failed_report.contains("before cleanup completed"));

            assert_eq!(failed.reason(), AktorStartReason::Init);
            let wrapped = std::fmt::Error.er_save("startup", failed_report);
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

    let (handle, mut owner) = local::channel::<(), String>(NonZeroUsize::MIN);
    let completion = owner.completion();
    let disposed = Rc::new(Cell::new(0));
    let hook = Released(disposed.clone(), owner.completion());
    let cleanup = Released(disposed.clone(), owner.completion());

    owner.hooks.before_each = Some(Box::new(move |_, _| {
        std::hint::black_box(&hook);
    }));
    let result = AssertUnwindSafe(owner.run_with(
        async || -> Result<(), (String, String)> { panic!("retained initialization sentinel") },
        async move |_| {
            std::hint::black_box(&cleanup);
            Ok(())
        },
    ))
    .catch_unwind()
    .await;

    assert!(result.is_err());
    let (ready, _) = handle.ready().await.unwrap_err();
    let (completed, _) = completion.wait_with_data().await.unwrap_err();

    assert_eq!(disposed.get(), 2);
    assert!(matches!(&*completed.cause, local::OwnerError::SetupPanic));
    assert!(matches!(ready, local::OwnerError::SetupPanic));

    let (handle, owner) = local::channel::<(), String>(NonZeroUsize::MIN);
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
