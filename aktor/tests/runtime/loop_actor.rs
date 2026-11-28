use super::*;
use std::{convert::Infallible, rc::Rc};

fn start<Actors: AktorStart<Group = AktorGroup>>(
    actors: Actors,
    grace: Duration,
) -> AktorStarted<Actors::Handles, AktorGroup, Actors::Error> {
    aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors,
        shutdown: |_| {},
        grace,
    })
}

#[tokio::test]
async fn stop() {
    // Holds a !Send Rc across awaits on its own thread.
    let capture = start(
        AktorLoop {
            name: "capture".into(),
            kind: AktorKind::TokioThread,
            work: async move |mut stopping| {
                let local = Rc::new(42_u32);
                stopping.stopped().await;
                Ok::<_, (Infallible, String)>(*local)
            },
            stuck: AktorStuck::Kill,
        },
        Duration::from_secs(5),
    );

    let (started, capture) = capture.into_parts();
    started.ready().await.unwrap();

    let observer = capture.clone();
    tokio::spawn(started.stop())
        .await
        .unwrap()
        .into_result()
        .unwrap();

    assert_eq!(capture.result().await, Ok(42));
    assert_eq!(observer.result().await, Ok(42));
}

#[tokio::test]
async fn reports_error() {
    let encoder = start(
        AktorLoop {
            name: "encoder".into(),
            kind: AktorKind::TokioThread,
            work: async move |_| Err::<(), _>(("device lost", "device lost".into())),
            stuck: AktorStuck::Kill,
        },
        Duration::from_secs(5),
    );

    let (started, encoder) = encoder.into_parts();
    let (end, message) = encoder.result().await.unwrap_err();
    let AktorLoopEnd::Failed("device lost", id) = end else {
        panic!("{end}");
    };
    assert!(message.contains("device lost"));

    let report = started.stop().await;
    let failure = report.failure(id).unwrap();
    assert!(matches!(
        failure.source,
        aktor::AktorFailureSource::Actor {
            phase: aktor::AktorPhase::Loop,
            ..
        }
    ));
    assert!(failure.error.contains("device lost"));
}

#[tokio::test]
async fn sibling_errors() {
    let (release, released) = tokio::sync::oneshot::channel();
    let first = AktorLoop {
        name: "encoder".into(),
        kind: AktorKind::TokioThread,
        work: async move |_| {
            released.await.unwrap();
            Err::<(), _>(("device lost", "device lost".into()))
        },
        stuck: AktorStuck::Kill,
    };
    let second = AktorLoop {
        name: "encoder".into(),
        kind: AktorKind::TokioTask,
        work: async move |mut stopping| {
            stopping.stopped().await;
            Err::<(), _>(("device lost", "device lost".into()))
        },
        stuck: AktorStuck::Kill,
    };
    let (group, handles) = start((first, second), Duration::from_secs(5)).into_parts();
    drop(handles);
    group.ready().await.unwrap();
    release.send(()).unwrap();

    let report = group.completion().wait().await.unwrap();
    let [first, second] = report.failures.as_slice() else {
        panic!("{report}");
    };

    for (failure, expected) in [
        (first, AktorExecution::TokioThread),
        (second, AktorExecution::TokioTask),
    ] {
        assert!(
            matches!(&failure.source, aktor::AktorFailureSource::Actor { kind, phase: aktor::AktorPhase::Loop, .. } if *kind == expected)
        );
    }
}

#[tokio::test]
async fn panics() {
    let decoder = start(
        AktorLoop {
            name: "decoder".into(),
            kind: AktorKind::TokioTask,
            work: |_| -> core::future::Ready<Result<(), (Infallible, String)>> {
                panic!("decoder exploded");
            },
            stuck: AktorStuck::Kill,
        },
        Duration::from_secs(5),
    );

    let Err((AktorLoopEnd::Panicked(_), message)) = decoder.into_parts().1.result().await else {
        panic!("the loop should have panicked");
    };

    assert!(message.contains("decoder exploded"));
}

#[tokio::test]
async fn refused() {
    let stopped = AktorGroup::new();
    let _ = stopped.stop().await;

    for group in [AktorGroup::new(), stopped] {
        let setup = AktorLoop {
            name: "capture".into(),
            kind: AktorKind::TokioThread,
            work: async |_| Ok::<_, (Infallible, String)>(()),
            stuck: AktorStuck::Kill,
        };
        let capture = group.spawn(setup);

        assert!(matches!(
            capture.into_parts(),
            Err((aktor::AktorStartError::Refused { .. }, _))
        ));
    }
}

#[tokio::test]
async fn leave() {
    let (release, released) = tokio::sync::oneshot::channel::<()>();

    let gpu = start(
        AktorLoop {
            name: "gpu".into(),
            kind: AktorKind::TokioThread,
            work: async move |mut stopping| {
                stopping.stopped().await;
                let _ = released.await;
                Err::<(), _>(("flush", "late flush failed".into()))
            },
            stuck: AktorStuck::Leave,
        },
        Duration::from_millis(50),
    );

    let (started, gpu) = gpu.into_parts();
    started.ready().await.unwrap();

    let stopped = tokio::time::timeout(Duration::from_secs(2), started.stop())
        .await
        .unwrap();
    assert!(!stopped.released());
    assert!(started.stop_released().now_or_never().is_none());

    release.send(()).unwrap();
    let report = tokio::time::timeout(Duration::from_secs(2), started.stop_released())
        .await
        .unwrap()
        .unwrap()
        .into_report();
    assert!(report.released());
    assert_eq!(report.stop_cause, aktor::StopCause::Requested);
    let [failure] = report.failures.as_slice() else {
        panic!("{report}");
    };
    assert!(failure.error.contains("late flush failed"));
    assert!(matches!(
        gpu.result().await,
        Err((AktorLoopEnd::Failed("flush", _), _))
    ));
}

#[tokio::test]
async fn leave_after_blocking_call() {
    let grace = Duration::from_millis(20);
    let (release, released) = std::sync::mpsc::channel();
    let (entered, blocking) = tokio::sync::oneshot::channel();

    let setup = AktorLoop {
        name: "gpu".into(),
        kind: AktorKind::TokioThread,
        work: async move |_| {
            entered.send(()).unwrap();

            released.recv().unwrap();

            // The owner thread can open the cleanup gate only after this poll yields.
            let (release, released) = tokio::sync::oneshot::channel();
            tokio::spawn(async move { release.send(42).ok() });
            Ok::<_, (Infallible, String)>(released.await.unwrap())
        },
        stuck: AktorStuck::Leave,
    };
    let gpu = start(setup, grace);
    blocking.await.unwrap();

    let _ = gpu.stop().await;
    release.send(()).unwrap();
    assert_eq!(gpu.into_parts().1.result().await, Ok(42));
}

#[tokio::test]
async fn kill() {
    let gpu = start(
        AktorLoop {
            name: "gpu".into(),
            kind: AktorKind::TokioThread,
            work: async move |mut stopping| {
                stopping.stopped().await;
                core::future::pending::<Result<(), (Infallible, String)>>().await
            },
            stuck: AktorStuck::Kill,
        },
        Duration::from_millis(40),
    );

    let _ = gpu.stop().await;

    assert!(matches!(
        gpu.into_parts().1.result().await,
        Err((AktorLoopEnd::Cut, _))
    ));
}

#[tokio::test]
async fn task() {
    let task = start(
        AktorLoop {
            name: "task".into(),
            kind: AktorKind::TokioTask,
            work: async move |mut stopping| {
                stopping.stopped().await;
                Ok::<_, (Infallible, String)>(7_u32)
            },
            stuck: AktorStuck::Kill,
        },
        Duration::from_secs(5),
    );

    task.ready().await.unwrap();
    let _ = task.stop().await;

    assert_eq!(task.into_parts().1.result().await, Ok(7));
}
