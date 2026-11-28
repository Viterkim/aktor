use super::*;
use core::num::NonZeroUsize;
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
    let Ok(case) = std::env::var("AKTOR_TEST_CASE") else {
        for case in ["queue", "hooks", "intervals", "wake"] {
            let output = support::run_test("task::runtime", case);
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
    let entered = Arc::new(Notify::new());
    let (handle, group) = runtime.block_on(async {
        let before = (case == "hooks").then_some(()).map(|_| InputDrop);
        let after = (case == "hooks").then_some(()).map(|_| InputDrop);
        let mut hooks = AktorHooks {
            before_each: move |_: &mut u32, _| {
                std::hint::black_box(&before);
            },
            after_each: move |_: &mut u32, _| {
                std::hint::black_box(&after);
            },
            intervals: Vec::new(),
        };

        if case == "intervals" {
            for _ in 0..2 {
                let capture = InputDrop;

                hooks.intervals.push(
                    AktorInterval::new(Duration::from_secs(60), move |_: AktorTaskState<u32>| {
                        std::hint::black_box(&capture);
                        async {}
                    })
                    .unwrap(),
                );
            }
        }

        let started = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "cancelled queue".into(),
                kind: AktorKind::TokioTask,
                start: aktor::AktorInit::Ready(async || 0_u32),
                end: (),
                hooks,
                capacity: NonZeroUsize::new(2).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(1),
        });
        started.ready().await.unwrap();
        let (group, handle) = started.into_parts();

        drop(pending(&handle, entered.clone()).send().await.unwrap());
        entered.notified().await;

        if matches!(&*case, "queue" | "wake") {
            queued(&handle, InputDrop).cast().await.unwrap();
            queued(&handle, InputDrop).cast().await.unwrap();
        }

        (handle, group)
    });
    let complete = group.completion();
    let mut waiting = Box::pin(pending(&handle, Arc::new(Notify::new())).send());

    if case == "wake" {
        let waker = std::task::Waker::from(Arc::new(QueueWake));
        let context = &mut std::task::Context::from_waker(&waker);

        assert!(waiting.as_mut().poll(context).is_pending());
    }

    drop(runtime);
    drop(group);

    let report = executor::block_on(complete.wait()).unwrap();
    assert!(report.failed(), "{report}");
    assert!(!report.ran_past_grace(), "{report}");
    let text = report.to_string();
    assert_eq!(text.matches("input drop failed").count(), 2, "{text}");
    if case == "wake" {
        assert_eq!(text.matches("queue wake failed").count(), 1, "{text}");
    }
    drop(waiting);
    drop(handle);
}

#[tokio::test]
async fn cancellation() {
    if std::env::var("AKTOR_TEST_CASE").is_err() {
        let output = support::run_test("task::cancellation", "task");

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
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "cancelled task".into(),
            kind: AktorKind::TokioTask,
            start: aktor::AktorInit::Ready(async || 0_u32),
            end: async move |state: u32| {
                *saved.lock().unwrap() = Some(state);
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_millis(400),
    });
    actors.ready().await.unwrap();

    let entered = Arc::new(Notify::new());

    drop(
        pending(actors.handles(), entered.clone())
            .send()
            .await
            .unwrap(),
    );
    entered.notified().await;

    let report = actors.stop().await;

    assert!(report.ran_past_grace());
    assert_eq!(cleaned.lock().unwrap().take(), Some(85));
    assert_eq!(
        actors.completion().wait().await.unwrap().actors[0].kind,
        AktorExecution::TokioTask
    );
}
