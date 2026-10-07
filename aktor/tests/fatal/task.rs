use super::*;
use std::time::Duration;
use tokio::sync::Notify;

#[aktor]
async fn pending(state: &mut u32, entered: Arc<Notify>) {
    *state = 85;
    entered.notify_one();
    core::future::pending::<()>().await;
}

struct InputDrop;
impl Drop for InputDrop {
    fn drop(&mut self) {
        eprintln!("input dropped");
        panic!("input drop failed");
    }
}

#[aktor]
async fn queued(_: &mut u32, _: InputDrop) {}

struct QueueWake;
impl std::task::Wake for QueueWake {
    fn wake(self: Arc<Self>) {
        panic!("queue wake failed");
    }
}

#[test]
fn runtime() {
    let Ok(case) = std::env::var("AKTOR_CHILD") else {
        for case in ["queue", "hooks", "intervals", "unpolled", "wake"] {
            let output = support::child("task::runtime", case);
            assert!(
                output.status.success(),
                "{case}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        return;
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = AktorGroup::with_grace(Duration::from_secs(1));
    let entered = Arc::new(Notify::new());
    let handle = runtime.block_on(async {
        group.start_threaded().unwrap();
        let unpolled = case == "unpolled";
        let starting = unpolled.then(|| InputDrop);
        let start = async move || {
            std::hint::black_box(&starting);
            Ok(0_u32)
        };
        let mut closures: AktorClosures<u32, _, AktorKind::TokioTask> = AktorClosures {
            start,
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        };

        if case == "hooks" {
            let before = InputDrop;
            let after = InputDrop;

            closures.before_each = Some(
                (move |_: &mut u32, _| {
                    std::hint::black_box(&before);
                })
                .into(),
            );
            closures.after_each = Some(
                (move |_: &mut u32, _| {
                    std::hint::black_box(&after);
                })
                .into(),
            );
        }

        if case == "intervals" {
            for _ in 0..2 {
                let capture = InputDrop;

                closures.intervals.push(AktorInterval {
                    every: Duration::from_secs(60),
                    run: (move |_: AktorTaskState<u32>| {
                        std::hint::black_box(&capture);
                        async {}
                    })
                    .into(),
                });
            }
        }

        if unpolled {
            let capture = InputDrop;

            closures.end = Some(
                (async move |_: u32| {
                    std::hint::black_box(&capture);
                    Ok(())
                })
                .into(),
            );
        }

        let mut starting = Box::pin(aktor_start_in(
            &group,
            AktorNew {
                name: AktorName::new("cancelled queue"),
                role: AktorNoRole,
                kind: AktorKind::TokioTask,
                closures,
                options: AktorNewOptions { capacity: 2 },
            },
        ));

        if unpolled {
            assert!(support::poll(&mut starting).is_pending());
            drop(starting);
            return None;
        }

        let handle = starting.await.unwrap();
        drop(pending(&handle, entered.clone()).send().await);
        entered.notified().await;

        if matches!(&*case, "queue" | "wake") {
            queued(&handle, InputDrop).cast().await;
            queued(&handle, InputDrop).cast().await;
        }

        Some(handle)
    });
    let complete = group.completion();
    let mut waiting = handle
        .as_ref()
        .map(|handle| Box::pin(pending(handle, Arc::new(Notify::new())).send()));

    if case == "wake" {
        let waker = std::task::Waker::from(Arc::new(QueueWake));
        let context = &mut std::task::Context::from_waker(&waker);

        assert!(
            waiting
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(context)
                .is_pending()
        );
    }

    drop(runtime);
    drop(group);

    let report = executor::block_on(complete.wait());
    assert!(report.failed(), "{report}");
    assert!(!report.timed_out, "{report}");
    assert_eq!(
        report.application.len(),
        if case == "wake" { 3 } else { 2 },
        "{report}"
    );
    drop(waiting);
    drop(handle);
}

#[tokio::test]
async fn cancellation() {
    if std::env::var("AKTOR_CHILD").is_err() {
        let output = support::child("task::cancellation", "task");

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let cleaned = Arc::new(Mutex::new(None));
    let saved = cleaned.clone();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("cancelled task"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(0_u32),
                end: Some(
                    (async move |state: u32| {
                        *saved.lock().unwrap() = Some(state);
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
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_millis(400),
        },
    })
    .await
    .unwrap();

    let entered = Arc::new(Notify::new());

    drop(pending(&actors.handles, entered.clone()).send().await);
    entered.notified().await;

    let report = actors.shutdown().await;

    assert!(report.timed_out);
    assert_eq!(cleaned.lock().unwrap().take(), Some(85));
    assert_eq!(
        actors.completion().wait().await.actors[0].kind,
        Some(AktorExecution::TokioTask)
    );
}
