use super::*;
use tokio::sync::oneshot;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test(flavor = "current_thread")]
async fn cleanup() -> TestResult {
    for outcome in [Ok(()), Err("serve failed")] {
        let (stop, stopping) = oneshot::channel();
        let (cleaning, cleanup_started) = oneshot::channel();
        let (release, released) = oneshot::channel();

        let (handle, actor) = spawn_runner(
            SpawnArgs {
                name: "cleanup".into(),
                capacity: 1,
                failure: FailurePolicy::Abort,
                setup: || Ok::<_, &'static str>(0usize),
                cleanup: move |_, result| {
                    assert_eq!(result, outcome.map_err(RunError::Failed));
                    cleaning.send(()).unwrap();
                    released.blocking_recv().unwrap();

                    result
                },
            },
            move |listener, _| {
                stopping.blocking_recv().unwrap();
                listener.close();
                drop(listener.try_recv().unwrap());

                outcome
            },
        )
        .await
        .unwrap();

        let mut queued = Box::pin(call(
            &handle,
            |_: &mut usize, ()| panic!("queued work ran"),
            (),
        ));
        let mut waiting = Box::pin(call(
            &handle,
            |_: &mut usize, ()| panic!("waiting work ran"),
            (),
        ));
        assert!(poll(queued.as_mut()).is_pending());
        assert!(poll(waiting.as_mut()).is_pending());

        stop.send(()).unwrap();
        cleanup_started.await?;

        assert!(poll(queued.as_mut()).is_pending());
        assert!(poll(waiting.as_mut()).is_pending());

        release.send(()).unwrap();
        let (finished, queued, waiting) =
            tokio::join!(actor.join_async(), panics(queued), panics(waiting));

        assert_eq!(finished?, outcome.map_err(RunError::Failed));
        assert!(queued);
        assert!(waiting);
    }

    Ok(())
}

#[tokio::test]
async fn error() {
    for abandon in [false, true] {
        let (cleaning, cleaned) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let mut gate = Some((cleaning, released));

        let (_handle, actor, thread) = spawn(SpawnArgs {
            name: "cleanup".into(),
            capacity: 1,
            failure: FailurePolicy::Unwind,
            setup: || Ok::<_, &'static str>(()),
            cleanup: move |_| {
                let (cleaning, released) = gate.take().unwrap();
                cleaning.send(()).unwrap();
                released.recv().unwrap();

                Err("save failed")
            },
        })
        .await
        .unwrap();

        let mut stopping = Box::pin(actor.shutdown());
        assert!(poll(stopping.as_mut()).is_pending());
        cleaned.await.unwrap();
        assert!(poll(stopping.as_mut()).is_pending());

        if abandon {
            drop(stopping);
            release.send(()).unwrap();
        } else {
            release.send(()).unwrap();
            assert!(
                matches!(stopping.await, Err(LifecycleError::Failed(error)) if *error == "save failed")
            );
        }

        assert_eq!(
            *thread.join_async().await.unwrap().unwrap_err().errors[0],
            "save failed"
        );
    }
}
