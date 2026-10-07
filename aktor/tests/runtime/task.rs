use aktor::*;
use core::{future::Future, pin::Pin};
use futures_util::FutureExt;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

struct Counter(std::cell::RefCell<u32>);

#[aktor]
async fn task_add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    if by == 0 {
        return Err("give me something");
    }

    *counter.0.get_mut() += by;
    Ok(*counter.0.get_mut())
}

#[aktor]
async fn task_nested<T: Into<u32>>(counter: &mut Counter, by: T) -> Result<u32, &'static str> {
    task_add(counter, by.into()).await
}

#[aktor]
async fn task_indices(counter: &mut Counter) -> impl Iterator<Item = u32> + Send {
    0..*counter.0.get_mut()
}

#[aktor]
async fn task_wait(counter: &mut Counter, entered: Arc<Notify>, release: Arc<Notify>) -> u32 {
    *counter.0.get_mut() += 1;
    entered.notify_one();
    release.notified().await;
    *counter.0.get_mut()
}

#[aktor]
async fn task_echo<T: 'static>(_: &mut Counter, value: T) -> T {
    value
}

#[aktor]
async fn task_echo_fixed(counter: &mut Counter, value: u32) -> u32 {
    task_echo(counter, value).await
}

#[tokio::test(flavor = "current_thread")]
async fn cooperative() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("task admission"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(Counter(std::cell::RefCell::new(0))),
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 1024 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    let sent = std::cell::Cell::new(0);
    let producer = async {
        for _ in 0..1024 {
            drop(task_add(&actors.handles, 1).send().await);
            sent.set(sent.get() + 1);
        }
    };

    let (_, observed) = tokio::join!(biased; producer, async { sent.get() });

    assert_eq!(task_add(&actors.handles, 1).await, Ok(1025));

    let report = actors.shutdown().await;

    assert!(!report.failed(), "{report}");
    assert!(observed < 1024, "submission monopolised the executor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let before = log.clone();
    let after = log.clone();
    let end = log.clone();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("counter task"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(Counter(std::cell::RefCell::new(0))),
                end: Some(
                    (async move |mut counter: Counter| {
                        end.lock().unwrap().push(("end", *counter.0.get_mut()));
                        Ok(())
                    })
                    .into(),
                ),
                intervals: vec![],
                before_each: Some(
                    (move |counter: &mut Counter, _: operation::Operation| {
                        before
                            .lock()
                            .unwrap()
                            .push(("before", *counter.0.get_mut()));
                    })
                    .into(),
                ),
                after_each: Some(
                    (move |counter: &mut Counter, _: operation::Operation| {
                        after.lock().unwrap().push(("after", *counter.0.get_mut()));
                    })
                    .into(),
                ),
            },
            options: AktorNewOptions { capacity: 1 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    let untouched = task_add(&actors.handles, 100);

    drop(untouched);
    assert_eq!(task_nested(&actors.handles, 2u8).await, Ok(2));
    assert_eq!(task_add(&actors.handles, 0).await, Err("give me something"));
    assert_eq!(
        task_echo(&actors.handles, (85_u32, std::marker::PhantomPinned))
            .await
            .0,
        85
    );
    assert_eq!(
        task_indices(&actors.handles).await.collect::<Vec<_>>(),
        [0, 1]
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    drop(
        task_wait(&actors.handles, entered.clone(), release.clone())
            .send()
            .await,
    );

    entered.notified().await;

    let (input, mut output) = task_echo(&actors.handles, 1u8).latest();

    input.send(2);
    input.send(3);

    let mut request = task_add(&actors.handles, 2).into_request();

    assert!((&mut request).now_or_never().is_none());

    let next = request.send().await;
    assert!(task_add(&actors.handles, 10).now_or_never().is_none());
    release.notify_one();
    assert_eq!(next.await, Ok(5));
    assert_eq!(output.next().await, Some(3));

    let (fixed, mut fixed_output) = task_echo_fixed::latest(&actors.handles);

    fixed.send(85);
    assert_eq!(fixed_output.next().await, Some(85));
    drop((fixed, fixed_output));

    let mut delayed = task_wait(&actors.handles, entered.clone(), release.clone())
        .send()
        .await;

    entered.notified().await;
    assert!(delayed.try_take().is_none());
    assert!(
        delayed
            .timeout(Duration::from_millis(1))
            .await
            .unwrap_err()
            .admitted
    );
    input.send(4);
    input.send(5);

    let shutdown = actors.shutdown();

    tokio::pin!(shutdown);
    assert!(shutdown.as_mut().now_or_never().is_none());
    release.notify_one();

    let report = shutdown.await;

    assert_eq!(delayed.try_take(), Some(6));
    assert_eq!(delayed.try_take(), None);
    assert_eq!(output.next().await, Some(5));
    assert_eq!(output.next().await, None);
    assert_eq!(output.next().await, None);
    assert!(!report.failed(), "{report}");
    assert_eq!(report.actors[0].kind, Some(AktorExecution::TokioTask));
    assert_eq!(log.lock().unwrap().last(), Some(&("end", 6)));
    assert!(task_add(&actors.handles, 1).now_or_never().is_none());
}

#[tokio::test]
async fn intervals() {
    let tick = Arc::new(Notify::new());
    let ping = tick.clone();
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("interval task"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(Counter(std::cell::RefCell::new(0))),
                end: None,
                intervals: vec![AktorInterval {
                    every: Duration::from_millis(10),
                    run: (move |mut counter: AktorTaskState<Counter>| {
                        let ping = ping.clone();
                        async move {
                            *counter.0.get_mut() += 1;
                            ping.notify_one();
                        }
                    })
                    .into(),
                }],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(2), tick.notified())
        .await
        .unwrap();
    assert!(task_add(&actors.handles, 1).await.unwrap() >= 2);

    let report = actors.shutdown().await;

    assert!(!report.failed(), "{report}");
}

struct PanicPayload;
impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("panic payload drop failed");
    }
}

#[aktor]
async fn task_fail(counter: &mut Counter) {
    *counter.0.get_mut() = 7;
    std::panic::panic_any(PanicPayload);
}

#[tokio::test]
async fn cleanup() {
    let cleaned = Arc::new(Mutex::new(None));
    let saved = cleaned.clone();
    let payload = PanicPayload;
    let actors = aktor_start(AktorSetup {
        actors: (
            AktorNew {
                name: AktorName::new("failed task"),
                role: AktorNoRole,
                kind: AktorKind::TokioTask,
                closures: AktorClosures {
                    start: async || Ok(Counter(std::cell::RefCell::new(0))),
                    end: Some(
                        (async move |mut counter: Counter| {
                            *saved.lock().unwrap() = Some(*counter.0.get_mut());
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
            AktorNew {
                name: AktorName::new("hook drop task"),
                role: AktorNoRole,
                kind: AktorKind::TokioTask,
                closures: AktorClosures {
                    start: async || Ok(Counter(std::cell::RefCell::new(0))),
                    end: None,
                    intervals: vec![],
                    before_each: Some(
                        (move |_: &mut Counter, _: operation::Operation| {
                            std::hint::black_box(&payload);
                        })
                        .into(),
                    ),
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    let (_input, mut output) = task_echo_fixed::latest(&actors.handles.1);

    assert!(output.next().now_or_never().is_none());

    let reply = task_fail(&actors.handles.0).send().await;
    let report = tokio::time::timeout(Duration::from_secs(2), actors.completion().wait())
        .await
        .unwrap();

    assert_eq!(cleaned.lock().unwrap().take(), Some(7));
    assert!(report.failure.as_ref().unwrap().phase.contains("task_fail"));
    assert!(!report.timed_out);
    assert!(output.next().now_or_never().is_none());

    let hook = report
        .actors
        .iter()
        .find(|actor| actor.actor == "hook drop task")
        .unwrap();

    assert!(
        hook.diagnostics
            .iter()
            .any(|error| error.to_string().contains("task hook drop"))
    );
    assert!(reply.now_or_never().is_none());
}

#[cfg(feature = "bevy")]
#[tokio::test]
async fn mixed_cleanup() {
    let pool = bevy_tasks::TaskPoolBuilder::new().num_threads(1).build();

    for bevy_first in [false, true] {
        let ended = Arc::new(Mutex::new(false));
        let end = ended.clone();
        let tokio = AktorNew {
            name: AktorName::new("Tokio counter"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(0_u32),
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        };

        let bevy = AktorNew {
            name: AktorName::new("Bevy counter"),
            role: AktorNoRole,
            kind: AktorKind::BevyTask(&pool),
            closures: AktorClosures {
                start: async || Ok(0_u32),
                end: Some(
                    (async move |_: u32| {
                        let mut yielded = false;

                        core::future::poll_fn(|cx| {
                            if yielded {
                                std::task::Poll::Ready(())
                            } else {
                                yielded = true;
                                cx.waker().wake_by_ref();
                                std::task::Poll::Pending
                            }
                        })
                        .await;
                        *end.lock().unwrap() = true;
                        Ok(())
                    })
                    .into(),
                ),
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        };

        let report = if bevy_first {
            aktor_start(AktorSetup {
                actors: (bevy, tokio),
                shutdown: |_| {},
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
            })
            .await
            .unwrap()
            .shutdown()
            .await
        } else {
            aktor_start(AktorSetup {
                actors: (tokio, bevy),
                shutdown: |_| {},
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
            })
            .await
            .unwrap()
            .shutdown()
            .await
        };

        assert!(!report.failed(), "{report}");
        assert!(*ended.lock().unwrap());
    }
}

#[tokio::test]
async fn admission_wait() {
    tokio::time::timeout(Duration::from_secs(3), async {
        #[cfg(feature = "bevy")]
        let pool = bevy_tasks::TaskPoolBuilder::new().num_threads(1).build();
        let modes = if cfg!(feature = "bevy") { 2 } else { 1 };

        for mode in 0..modes {
            let setup = AktorNew {
                name: AktorName::new("task admission"),
                role: AktorNoRole,
                kind: AktorKind::TokioTask,
                closures: AktorClosures {
                    start: async || Ok(Counter(std::cell::RefCell::new(0))),
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 1 },
            };

            #[cfg(feature = "bevy")]
            let actors = if mode == 1 {
                aktor_start(AktorSetup {
                    actors: AktorNew {
                        name: setup.name,
                        role: setup.role,
                        kind: AktorKind::BevyTask(&pool),
                        closures: AktorClosures {
                            start: setup.closures.start,
                            end: None,
                            intervals: vec![],
                            before_each: None,
                            after_each: None,
                        },
                        options: setup.options,
                    },
                    shutdown: |_| {},
                    options: AktorOptions {
                        shutdown_grace: Duration::from_secs(5),
                    },
                })
                .await
                .unwrap()
            } else {
                aktor_start(AktorSetup {
                    actors: setup,
                    shutdown: |_| {},
                    options: AktorOptions {
                        shutdown_grace: Duration::from_secs(5),
                    },
                })
                .await
                .unwrap()
            };

            #[cfg(not(feature = "bevy"))]
            let actors = {
                let _ = mode;
                aktor_start(AktorSetup {
                    actors: setup,
                    shutdown: |_| {},
                    options: AktorOptions {
                        shutdown_grace: Duration::from_secs(5),
                    },
                })
                .await
                .unwrap()
            };

            let entered = Arc::new(Notify::new());
            let first_gate = Arc::new(Notify::new());
            let second_gate = Arc::new(Notify::new());
            let first = task_wait(&actors.handles, entered.clone(), first_gate.clone())
                .send()
                .await;

            entered.notified().await;

            let mut second = task_wait(&actors.handles, entered.clone(), second_gate.clone())
                .send()
                .await;
            let mut request = task_add(&actors.handles, 1).into_request();

            assert!((&mut request).now_or_never().is_none());

            assert!((&mut request).now_or_never().is_none());
            first_gate.notify_one();
            entered.notified().await;

            let count = Arc::new(super::support::CountWake::default());
            let waker = std::task::Waker::from(count.clone());

            assert!(
                Pin::new(&mut second)
                    .poll(&mut std::task::Context::from_waker(&waker))
                    .is_pending()
            );
            assert!(second.try_take().is_none());

            let next = request.send().await;

            second_gate.notify_one();

            assert_eq!(first.await, 1);
            assert_eq!(next.await, Ok(3));
            assert!(
                count.0.load(std::sync::atomic::Ordering::Relaxed) > 0,
                "try_take replaced the task reply waker"
            );
            assert_eq!(second.try_take(), Some(2));
            assert_eq!(second.try_take(), None);

            let held = task_wait(&actors.handles, entered.clone(), first_gate.clone())
                .send()
                .await;

            entered.notified().await;

            let queued = task_add(&actors.handles, 1).send().await;
            let mut waiting = task_add(&actors.handles, 1);
            assert!((&mut waiting).now_or_never().is_none());

            first_gate.notify_one();
            assert_eq!(waiting.await, Ok(6));
            assert_eq!(held.await, 4);
            assert_eq!(queued.await, Ok(5));

            let held = task_wait(&actors.handles, entered.clone(), first_gate.clone())
                .send()
                .await;

            entered.notified().await;

            let queued = task_add(&actors.handles, 1).send().await;
            let mut waiting = task_add(&actors.handles, 1).into_request();

            assert!((&mut waiting).now_or_never().is_none());
            actors.killswitch().stop();

            assert!((&mut waiting).now_or_never().is_none());
            drop(waiting);

            first_gate.notify_one();
            assert_eq!(held.await, 7);
            assert_eq!(queued.await, Ok(8));
            assert!(!actors.shutdown().await.failed());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn parked_admission() {
    for assigned in [false, true] {
        for mode in 0..4 {
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let actors = aktor_start(AktorSetup {
                actors: AktorNew {
                    name: AktorName::new("parked admission"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioTask,
                    closures: AktorClosures {
                        start: async || Ok(Counter(std::cell::RefCell::new(0))),
                        end: None,
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: AktorNewOptions { capacity: 1 },
                },
                shutdown: |_| {},
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(5),
                },
            })
            .await
            .unwrap();

            let first = task_wait(&actors.handles, entered.clone(), release.clone())
                .send()
                .await;

            entered.notified().await;

            let second = task_wait(&actors.handles, entered.clone(), release.clone())
                .send()
                .await;
            let request = task_add(&actors.handles, 100);
            let mut waiting: Pin<Box<dyn Future<Output = ()> + '_>> = match mode {
                0 => Box::pin(async {
                    request.await.unwrap();
                }),
                1 => Box::pin(async {
                    request.send().await.await.unwrap();
                }),
                2 => Box::pin(task_echo(&actors.handles, ()).cast()),
                _ => Box::pin(async {
                    request
                        .timeout(Duration::from_secs(60))
                        .await
                        .unwrap()
                        .unwrap();
                }),
            };

            assert!(waiting.as_mut().now_or_never().is_none());

            if assigned {
                release.notify_one();
                entered.notified().await;
            }

            actors.killswitch().stop();
            assert!(waiting.as_mut().now_or_never().is_none());

            if !assigned {
                release.notify_one();
                entered.notified().await;
            }

            release.notify_one();

            let report = tokio::time::timeout(Duration::from_secs(1), actors.completion())
                .await
                .expect("unadmitted waiter kept shutdown alive");

            assert!(!report.failed(), "{report}");
            assert_eq!(first.await, 1);
            assert_eq!(second.await, 2);
            assert!(waiting.as_mut().now_or_never().is_none());
        }
    }
}

#[tokio::test]
async fn consumed() {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("consumed task"),
            role: AktorNoRole,
            kind: AktorKind::TokioTask,
            closures: AktorClosures {
                start: async || Ok(Counter(std::cell::RefCell::new(0))),
                end: None,
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    let mut request = task_echo(&actors.handles, ()).into_request();
    (&mut request).await;

    let reused = std::panic::AssertUnwindSafe(async {
        drop(request.send().await);
    })
    .catch_unwind()
    .await;

    assert!(reused.is_err(), "consumed task returned another reply");

    assert!(!actors.shutdown().await.failed());
}
