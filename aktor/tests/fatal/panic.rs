use super::*;

#[tokio::test]
async fn lifecycle() {
    for command in ["serve", "pause", "shutdown"] {
        let cleaned = Arc::new(Mutex::new(None));
        let record = cleaned.clone();
        let (handle, actor, thread) = spawn(SpawnArgs {
            name: "failure".into(),
            capacity: 2,
            failure: FailurePolicy::Unwind,
            setup: || Ok::<_, std::convert::Infallible>(0usize),
            cleanup: move |state| {
                *record.lock().unwrap() = Some(state);
                Ok::<_, std::convert::Infallible>(())
            },
        })
        .await
        .unwrap();

        let (entered, entering) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();

        call(
            &handle,
            move |_, ()| {
                entered.send(()).unwrap();
                released.recv().unwrap();
            },
            (),
        )
        .cast()
        .await;
        entering.await.unwrap();

        crash(&handle).cast().await;

        let later = call(&handle, |state, ()| *state = 999, ()).send().await;

        let stopping = async {
            match command {
                "pause" => actor.pause().await,
                "shutdown" => actor.shutdown().await,
                _ => {
                    actor.closed().await;
                    Ok(())
                }
            }
        };

        let mut stopping = Box::pin(stopping);

        assert!(poll(stopping.as_mut()).is_pending());

        release.send(()).unwrap();

        let _result = stopping.await;

        handle.closed().await;
        assert_eq!(*cleaned.lock().unwrap(), Some(1));

        let error = thread.join_async().await.unwrap_err();
        let _: &(dyn std::error::Error + Send + Sync) = &error;

        assert_eq!(
            error.payload.lock().downcast_ref::<&str>(),
            Some(&"operation failed")
        );

        assert!(tokio::spawn(later).await.unwrap_err().is_panic());
    }
}

#[tokio::test]
async fn runner() {
    for cleanup_panics in [false, true] {
        let cleaned = Arc::new(Mutex::new(None));
        let record = cleaned.clone();
        let observed = cleaned.clone();
        let (notified, notification) = std::sync::mpsc::channel();

        let (handle, thread) = spawn_runner(
            SpawnArgs {
                name: "custom".into(),
                capacity: 1,
                failure: FailurePolicy::shutdown(move |failure| {
                    assert!(matches!(failure.kind, FailureKind::Operation(_)));
                    assert_eq!(
                        failure.payload.downcast_ref::<&str>(),
                        Some(&"operation failed")
                    );
                    assert_eq!(
                        failure
                            .diagnostics
                            .iter()
                            .any(|error| error.diagnostics.contains("cleanup failed too")),
                        cleanup_panics
                    );
                    notified.send(*observed.lock().unwrap()).unwrap();
                }),
                setup: || Ok::<_, std::convert::Infallible>(0usize),
                cleanup: move |state, outcome| {
                    assert_eq!(outcome, Err(RunError::Panicked));
                    *record.lock().unwrap() = Some(state);

                    if cleanup_panics {
                        panic!("cleanup failed too");
                    }
                },
            },
            |listener, state| {
                listener.serve_blocking(state);
                Ok(())
            },
        )
        .await
        .unwrap();

        crash(&handle).cast().await;
        handle.closed().await;

        assert!(thread.join_async().await.is_err());
        assert_eq!(notification.try_recv().unwrap(), Some(1));
    }
}
