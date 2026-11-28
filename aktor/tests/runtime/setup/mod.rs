#[cfg(feature = "local")]
mod drivers;
mod intervals;
#[cfg(any(feature = "local", feature = "std_thread"))]
mod lifecycle;
mod panics;
mod startup;

#[cfg(feature = "local")]
use super::support;
use aktor::*;
use core::num::NonZeroUsize;
use futures_util::FutureExt;
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

struct Counter(Rc<Cell<u32>>);

#[test]
fn caller_runtime() {
    let Ok(case) = std::env::var("AKTOR_TEST_CASE") else {
        for case in ["stop", "drop", "live", "task stop", "task runtime exit"] {
            let output = super::support::run_test("setup::caller_runtime", case);
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
    if case.starts_with("task ") {
        let (entered, running) = tokio::sync::oneshot::channel();
        let (finish, finishing) = tokio::sync::oneshot::channel();
        let (cleaned, cleanup) = tokio::sync::oneshot::channel();
        let actors = runtime.block_on(async {
            let actors = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "caller task".into(),
                    kind: AktorKind::TokioTask,
                    start: AktorInit::Ready(async || ()),
                    end: async move |_: ()| {
                        entered.send(()).unwrap();
                        finishing.await.unwrap();
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        cleaned.send(()).unwrap();
                    },
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(1),
            });

            actors.ready().await.unwrap();
            actors
        });
        let completion = actors.completion();
        actors.killswitch().stop();
        runtime.block_on(running).unwrap();

        if case == "task stop" {
            finish.send(()).unwrap();
            let report = runtime.block_on(completion.wait()).unwrap();

            assert!(!report.failed(), "{report}");
            runtime.block_on(cleanup).unwrap();
            drop(runtime);
        } else {
            drop(runtime);
            assert!(finish.send(()).is_err());
            assert!(executor::block_on(cleanup).is_err());

            let report = executor::block_on(completion.wait()).unwrap();

            assert!(report.failed(), "{report}");
            assert!(!report.ran_past_grace(), "{report}");
            assert!(
                report
                    .to_string()
                    .contains("actor task cancelled before cleanup completed")
            );
        }

        drop(actors);
        return;
    }

    let (entered, running) = std::sync::mpsc::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (cleaned, cleanup) = std::sync::mpsc::channel();
    let (finish, finishing) = tokio::sync::oneshot::channel();
    let mut finishing = Some(finishing);
    let caller_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let interval_closed = caller_closed.clone();
    let (ticked, ticks) = std::sync::mpsc::channel();
    let actors = runtime.block_on(async {
        let actors = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "caller runtime".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Ready(async || 0usize),
                end: async move |state: usize| {
                    cleaned.send(state).unwrap();
                    finishing.take().unwrap().await.unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                },
                hooks: AktorHooks {
                    intervals: if case == "live" {
                        vec![
                            AktorInterval::new(
                                Duration::from_millis(1),
                                async move |state: &mut usize| {
                                    if interval_closed.load(std::sync::atomic::Ordering::Acquire) {
                                        let _sent = ticked.send(*state);
                                    }
                                },
                            )
                            .unwrap(),
                        ]
                    } else {
                        Vec::new()
                    },
                    before_each: (),
                    after_each: (),
                },
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(1),
        });

        actors.ready().await.unwrap();
        actors
    });
    let handle = actors.handles().handle();
    let completion = actors.completion();
    let owner = actors.handles().completion();

    let reply = runtime.block_on(finish_after(&handle, entered, released).send());
    running.recv_timeout(Duration::from_secs(1)).unwrap();
    drop(reply);

    if case == "stop" {
        actors.killswitch().stop();
    }

    drop(runtime);
    caller_closed.store(true, std::sync::atomic::Ordering::Release);
    release.send(()).unwrap();

    if case == "live" {
        assert_eq!(ticks.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    }

    drop(actors);
    assert_eq!(cleanup.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
    finish.send(()).unwrap();

    let report = executor::block_on(completion.wait()).unwrap();
    assert!(!report.failed(), "{report}");
    assert!(executor::block_on(owner.wait()).is_ok());
    drop(handle);
}

#[aktor]
async fn finish_after(
    state: &mut usize,
    entered: std::sync::mpsc::Sender<()>,
    released: tokio::sync::oneshot::Receiver<()>,
) {
    entered.send(()).unwrap();
    released.await.unwrap();
    *state += 1;
}

mod aktors {
    pub struct Archive;
}

#[aktor(latest)]
async fn user_count(counter: &u32) -> u32 {
    *counter
}

#[aktor(role = aktors::Archive)]
async fn archive_count<T: From<u32>>(counter: &u32) -> T {
    T::from(*counter)
}

#[cfg(feature = "local")]
#[aktor(latest)]
async fn local_add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    add(counter, by).await
}

#[aktor(latest)]
async fn add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    if by == 0 {
        return Err("give me something");
    }

    let value = counter.0.get() + by;

    counter.0.set(value);
    Ok(value)
}

#[tokio::test]
async fn counter() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let before = log.clone();
    let after = log.clone();
    let end = log.clone();
    let setup = AktorNew {
        name: "BingoManden".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || Counter(Rc::new(Cell::new(0)))),
        end: async move |counter: Counter| {
            end.lock().unwrap().push(("end", counter.0.get()));
        },
        hooks: AktorHooks {
            intervals: vec![],
            before_each: move |counter: &mut Counter, operation: operation::Operation| {
                assert!(operation.name.contains("add"));
                before.lock().unwrap().push(("before", counter.0.get()));
            },
            after_each: move |counter: &mut Counter, _: operation::Operation| {
                after.lock().unwrap().push(("after", counter.0.get()));
            },
        },
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    let actors = tokio::spawn(async {
        let actors = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: setup,
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });

        actors.ready().await.unwrap();
        actors
    })
    .await
    .unwrap();
    let completion = actors.completion();

    assert!(completion.try_report().is_none());
    assert_eq!(add(actors.handles(), 5).await, Ok(Ok(5)));
    assert_eq!(add(actors.handles(), 0).await, Ok(Err("give me something")));

    let (input, mut output) = add::latest(actors.handles());

    input.send(3);
    assert_eq!(output.next().await, Some(Ok(Ok(8))));

    let report = actors.stop().await;

    assert!(!report.failed(), "{report}");
    assert!(!completion.try_report().unwrap().failed());
    assert_eq!(
        *log.lock().unwrap(),
        [
            ("before", 0),
            ("after", 5),
            ("before", 5),
            ("after", 5),
            ("before", 5),
            ("after", 8),
            ("end", 8),
        ]
    );
}

#[tokio::test]
async fn multiple() {
    fn counter(
        name: &str,
        fail: bool,
        cleaned: Arc<Notify>,
    ) -> impl AktorDefinition<Aktor<Counter>, std::io::Error> {
        AktorNew {
            name: name.into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Try(async move || {
                if fail {
                    Err((std::io::Error::other("no thanks"), "no thanks".into()))
                } else {
                    Ok(Counter(Rc::new(Cell::new(0))))
                }
            }),
            end: async move |_: Counter| {
                cleaned.notify_one();
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        }
    }

    let cleaned = Arc::new(Notify::new());
    let attempt = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: (
            counter("BingoManden", false, cleaned.clone()),
            counter("Haandboldfuglen", true, Arc::new(Notify::new())),
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    let result = attempt.ready().await;

    let (failure, _) = result.err().unwrap();
    assert!(matches!(
        failure,
        aktor::AktorStartError::Initializer {
            error: AktorBranch::Second(_),
            ..
        }
    ));
    assert!(matches!(
        failure
            .startup()
            .unwrap()
            .report
            .as_ref()
            .unwrap()
            .stop_cause,
        aktor::StopCause::StartupFailure(_)
    ));
    cleaned.notified().await;

    let actors = tokio::spawn(async {
        let actors = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: (
                AktorNew {
                    name: format!("users-{}", 1),
                    kind: AktorKind::TokioThread,
                    start: AktorInit::Ready(async || 1_u32),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
                AktorNew {
                    name: "two".into(),
                    kind: AktorKind::TokioThread,
                    start: AktorInit::Ready(async || 85_u32),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::new(32).unwrap(),
                    stuck: AktorStuck::Kill,
                },
            ),
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });

        actors.ready().await.unwrap();
        actors
    })
    .await
    .unwrap();

    let archive = actors.handles().1.clone().with_role::<aktors::Archive>();

    assert_eq!(user_count(&actors.handles().0).await, Ok(1));
    assert_eq!(archive_count::<u32, _>(&archive).await, Ok(85));

    let (input, mut output) = user_count::latest(&actors.handles().0);

    input.send();
    assert_eq!(output.next().await, Some(Ok(1)));
    assert!(!actors.stop().await.failed());
}

#[cfg(feature = "std_thread")]
#[test]
fn standard() {
    executor::block_on(async {
        let log = Arc::new(Mutex::new(Vec::new()));
        let before = log.clone();
        let after = log.clone();
        let end = log.clone();
        let owner_thread = Arc::new(Mutex::new(None));
        let started_thread = owner_thread.clone();
        let interval_thread = owner_thread.clone();
        let tick = Arc::new(Notify::new());
        let ping = tick.clone();

        let actors = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "standard counter".into(),
                kind: AktorKind::StdThread,
                start: AktorInit::Ready(async move || {
                    assert!(tokio::runtime::Handle::try_current().is_err());
                    *started_thread.lock().unwrap() = Some(std::thread::current().id());
                    Counter(Rc::new(Cell::new(0)))
                }),
                end: async move |counter: Counter| {
                    assert!(tokio::runtime::Handle::try_current().is_err());
                    assert_eq!(
                        *owner_thread.lock().unwrap(),
                        Some(std::thread::current().id())
                    );
                    end.lock().unwrap().push(("end", counter.0.get()));
                },
                hooks: AktorHooks {
                    intervals: vec![
                        AktorInterval::new(
                            Duration::from_millis(10),
                            async move |_: &mut Counter| {
                                assert!(tokio::runtime::Handle::try_current().is_err());
                                assert_eq!(
                                    *interval_thread.lock().unwrap(),
                                    Some(std::thread::current().id())
                                );
                                ping.notify_one();
                            },
                        )
                        .unwrap(),
                    ],
                    before_each: move |counter: &mut Counter, _: operation::Operation| {
                        before.lock().unwrap().push(("before", counter.0.get()));
                    },
                    after_each: move |counter: &mut Counter, _: operation::Operation| {
                        after.lock().unwrap().push(("after", counter.0.get()));
                    },
                },
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });
        actors.ready().await.unwrap();

        tokio::select! {
            _ = tick.notified() => {},
            _ = executor::sleep_until(std::time::Instant::now() + Duration::from_secs(2)) => {
                panic!("standard interval did not run");
            },
        }

        assert_eq!(add(actors.handles(), 2).await, Ok(Ok(2)));
        assert_eq!(add(actors.handles(), 0).await, Ok(Err("give me something")));

        let gate = Arc::new(Notify::new());
        let mut reply = held_add(actors.handles(), gate.clone())
            .send()
            .await
            .unwrap();

        use futures_util::FutureExt;
        assert!(reply.timeout(Duration::MAX).now_or_never().is_none());
        assert!(
            reply
                .timeout(Duration::from_millis(5))
                .await
                .unwrap_err()
                .admitted
        );
        gate.notify_one();
        assert_eq!(reply.await, Ok(3));

        let (input, mut output) = add::latest(actors.handles());

        input.send(4);
        assert_eq!(output.next().await, Some(Ok(Ok(7))));
        input.send(1);

        let report = actors.stop().await;

        assert!(!report.failed(), "{report}");
        assert_eq!(output.next().await, Some(Ok(Ok(8))));
        assert_eq!(output.next().await, None);
        assert_eq!(log.lock().unwrap().last(), Some(&("end", 8)));
        drop(input);
    });
}

#[aktor]
async fn held_add(counter: &mut Counter, gate: Arc<Notify>) -> u32 {
    counter.0.set(counter.0.get() + 1);
    gate.notified().await;
    counter.0.get()
}
