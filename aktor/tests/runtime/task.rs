use aktor::*;
use core::future::IntoFuture;
use core::num::NonZeroUsize;
use core::{future::Future, pin::Pin};
use futures_util::FutureExt;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, oneshot};

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

#[aktor(latest)]
async fn task_echo<T: 'static>(_: &mut Counter, value: T) -> T {
    value
}

#[aktor(latest)]
async fn task_echo_fixed(counter: &mut Counter, value: u32) -> u32 {
    task_echo(counter, value).await
}

#[test]
fn cancelled_before_first_poll() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (closed, closing) = std::sync::mpsc::channel();

    let started = runtime.block_on(async {
        aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "never polled".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || ()),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: move |_| {
                let _ = closed.send(());
            },
            grace: Duration::from_millis(100),
        })
    });
    drop(runtime);

    closing.recv_timeout(Duration::from_secs(2)).unwrap();
    let _ = aktor::executor::block_on(started.completion().wait_retired()).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cooperative() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "task admission".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(1024).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let sent = std::cell::Cell::new(0);
    let producer = async {
        for _ in 0..1024 {
            drop(task_add(actors.handles(), 1).send().await.unwrap());
            sent.set(sent.get() + 1);
        }
    };

    let (_, observed) = tokio::join!(biased; producer, async { sent.get() });

    assert_eq!(task_add(actors.handles(), 1).await, Ok(Ok(1025)));

    let report = actors.stop().await;

    assert!(!report.failed(), "{report}");
    assert!(observed < 1024, "submission monopolised the executor");
}

#[tokio::test]
async fn returned_lease() {
    type Lease = AktorTaskState<Counter>;
    type Exchange = (oneshot::Sender<Lease>, oneshot::Receiver<Lease>);

    async fn exchange(lease: Lease, (send, receive): Exchange) -> (Lease, ()) {
        assert!(send.send(lease).is_ok());
        (receive.await.unwrap(), ())
    }

    let hooks = Arc::new(Mutex::new(Vec::new()));
    let counter = |name: &str, value| AktorNew {
        name: name.into(),
        kind: AktorKind::TokioTask,
        start: AktorInit::Ready(async move || Counter(std::cell::RefCell::new(value))),
        end: (),
        hooks: AktorHooks {
            after_each: {
                let hooks = hooks.clone();
                move |state: &mut Counter, operation: operation::Operation| {
                    hooks
                        .lock()
                        .unwrap()
                        .push((operation.name, *state.0.get_mut()));
                }
            },
            before_each: (),
            intervals: Vec::new(),
        },
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    };
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: (counter("first", 0), counter("second", 10)),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let (first, second) = actors.handles();
    let (send_first, from_first) = oneshot::channel();
    let (send_second, from_second) = oneshot::channel();
    let operation = operation::Operation {
        name: "exchange",
        caller: core::panic::Location::caller(),
    };

    let (a, b) = tokio::join!(
        task::AktorTaskRequest::new(first, operation, (send_first, from_second), exchange),
        task::AktorTaskRequest::new(second, operation, (send_second, from_first), exchange),
    );
    a.unwrap();
    b.unwrap();
    assert!(hooks.lock().unwrap().is_empty());

    assert_eq!(
        tokio::join!(task_add(first, 1), task_add(second, 1)),
        (Ok(Ok(1)), Ok(Ok(11)))
    );

    let _ = actors.stop().await;
}

#[tokio::test]
async fn shutdown_with_detached_lease() {
    async fn detach(
        lease: AktorTaskState<Counter>,
        sent: oneshot::Sender<AktorTaskState<Counter>>,
    ) -> (AktorTaskState<Counter>, ()) {
        assert!(sent.send(lease).is_ok());
        core::future::pending().await
    }

    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "detached lease".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_millis(50),
    });
    actors.ready().await.unwrap();
    let handle = actors.handles().clone();
    let (sent, received) = oneshot::channel();
    let operation = operation::Operation {
        name: "detach",
        caller: core::panic::Location::caller(),
    };
    task::AktorTaskRequest::new(&handle, operation, sent, detach)
        .cast()
        .await
        .unwrap();
    let lease = received.await.unwrap();

    let report = actors.stop().await;
    assert!(report.ran_past_grace());
    assert_eq!(
        report.actors[0].retirement(),
        Some(AktorRetirement::StillRunning)
    );
    tokio::time::timeout(Duration::from_secs(2), handle.finished())
        .await
        .expect("owner stalled trying to recover its state");
    drop(lease);
}

#[tokio::test(start_paused = true)]
async fn lease_recovery_and_end_share_grace() {
    async fn detach(
        lease: AktorTaskState<()>,
        (sent, cancelled): (oneshot::Sender<AktorTaskState<()>>, Arc<Notify>),
    ) -> (AktorTaskState<()>, ()) {
        struct OnDrop(Arc<Notify>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let _cancelled = OnDrop(cancelled);
        assert!(sent.send(lease).is_ok());
        core::future::pending().await
    }

    let cancelled = Arc::new(Notify::new());
    let entering_end = Arc::new(Notify::new());
    let entered_end = entering_end.clone();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "cleanup grace".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || ()),
            end: async move |()| {
                entering_end.notify_one();
                tokio::time::sleep(Duration::from_secs(7)).await;
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(10),
    });
    actors.ready().await.unwrap();
    let (sent, received) = oneshot::channel();
    let operation = operation::Operation {
        name: "detach until cleanup",
        caller: core::panic::Location::caller(),
    };
    task::AktorTaskRequest::new(
        actors.handles(),
        operation,
        (sent, cancelled.clone()),
        detach,
    )
    .cast()
    .await
    .unwrap();
    let lease = received.await.unwrap();

    let closing = actors.completion();
    actors.killswitch().stop();
    cancelled.notified().await;
    tokio::time::advance(Duration::from_secs(6)).await;
    drop(lease);
    entered_end.notified().await;

    tokio::time::timeout(Duration::from_millis(4500), actors.handles().finished())
        .await
        .expect("cleanup got a new grace after recovering state");
    assert!(closing.await.unwrap().ran_past_grace());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let before = log.clone();
    let after = log.clone();
    let end = log.clone();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "counter task".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
            end: async move |mut counter: Counter| {
                end.lock().unwrap().push(("end", *counter.0.get_mut()));
            },
            hooks: AktorHooks {
                intervals: vec![],
                before_each: move |counter: &mut Counter, _: operation::Operation| {
                    before
                        .lock()
                        .unwrap()
                        .push(("before", *counter.0.get_mut()));
                },
                after_each: move |counter: &mut Counter, _: operation::Operation| {
                    after.lock().unwrap().push(("after", *counter.0.get_mut()));
                },
            },
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let untouched = task_add(actors.handles(), 100);

    drop(untouched);
    assert_eq!(task_nested(actors.handles(), 2u8).await, Ok(Ok(2)));
    assert_eq!(
        task_add(actors.handles(), 0).await,
        Ok(Err("give me something"))
    );
    assert_eq!(
        task_echo(actors.handles(), (85_u32, std::marker::PhantomPinned))
            .await
            .unwrap()
            .0,
        85
    );
    assert_eq!(
        task_indices(actors.handles())
            .await
            .unwrap()
            .collect::<Vec<_>>(),
        [0, 1]
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    drop(
        task_wait(actors.handles(), entered.clone(), release.clone())
            .send()
            .await
            .unwrap(),
    );

    entered.notified().await;

    let (input, mut output) = task_echo(actors.handles(), 1u8).latest();

    input.send(2);
    input.send(3);

    let mut request = task_add(actors.handles(), 2).into_future();

    assert!((&mut request).now_or_never().is_none());

    let next = request.send().await.unwrap();
    assert!(
        task_add(actors.handles(), 10)
            .into_future()
            .now_or_never()
            .is_none()
    );
    release.notify_one();
    assert_eq!(next.await, Ok(Ok(5)));
    assert_eq!(output.next().await, Some(Ok(3)));

    let (fixed, mut fixed_output) = task_echo_fixed::latest(actors.handles());

    fixed.send(85);
    assert_eq!(fixed_output.next().await, Some(Ok(85)));
    drop((fixed, fixed_output));

    let mut delayed = task_wait(actors.handles(), entered.clone(), release.clone())
        .send()
        .await
        .unwrap();

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

    let shutdown = actors.stop();

    tokio::pin!(shutdown);
    assert!(shutdown.as_mut().now_or_never().is_none());
    release.notify_one();

    let report = shutdown.await;

    assert_eq!(delayed.try_take(), Some(Ok(6)));
    assert_eq!(delayed.try_take(), None);
    assert_eq!(output.next().await, Some(Ok(5)));
    assert_eq!(output.next().await, None);
    assert!(!report.failed(), "{report}");
    assert_eq!(log.lock().unwrap().last(), Some(&("end", 6)));
    assert_eq!(
        task_add(actors.handles(), 1).await.unwrap_err().outcome,
        aktor::AktorDeliveryOutcome::NotAdmitted
    );
}

#[tokio::test]
async fn intervals() {
    let tick = Arc::new(Notify::new());
    let ping = tick.clone();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "interval task".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
            end: (),
            hooks: AktorHooks {
                intervals: vec![
                    AktorInterval::new(
                        Duration::from_millis(10),
                        move |mut counter: AktorTaskState<Counter>| {
                            let ping = ping.clone();
                            async move {
                                *counter.0.get_mut() += 1;
                                ping.notify_one();
                            }
                        },
                    )
                    .unwrap(),
                ],
                before_each: (),
                after_each: (),
            },
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), tick.notified())
        .await
        .unwrap();
    assert!(task_add(actors.handles(), 1).await.unwrap().unwrap() >= 2);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                biased;
                _ = tick.notified() => break,
                result = task_add(actors.handles(), 1) => { result.unwrap().unwrap(); },
            }
        }
    })
    .await
    .unwrap();

    let report = actors.stop().await;

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
        stuck: AktorGroupStuck::KeepRunning,
        actors: (
            AktorNew {
                name: "failed task".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
                end: async move |mut counter: Counter| {
                    *saved.lock().unwrap() = Some(*counter.0.get_mut());
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "hook drop task".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
                end: (),
                hooks: AktorHooks {
                    intervals: vec![],
                    before_each: move |_: &mut Counter, _: operation::Operation| {
                        std::hint::black_box(&payload);
                    },
                    after_each: (),
                },
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let (_input, mut output) = task_echo_fixed::latest(&actors.handles().1);

    assert!(output.next().now_or_never().is_none());

    let reply = task_fail(&actors.handles().0).send().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(2), actors.completion().wait())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(cleaned.lock().unwrap().take(), Some(7));
    assert!(
        matches!(&report.failures[0].source, aktor::AktorFailureSource::Actor { phase: aktor::AktorPhase::Call { name, .. }, .. } if name.contains("task_fail"))
    );
    assert!(!report.ran_past_grace());
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());

    assert!(report.failures.iter().any(|failure| matches!(&failure.source, aktor::AktorFailureSource::Actor { name, phase: aktor::AktorPhase::Drop, .. } if name == "hook drop task")));
    assert_eq!(
        reply.await.unwrap_err().failure_id,
        Some(report.failures[0].id)
    );

    for mode in 0..3 {
        let held = (mode == 2).then(|| PanicPayload);
        let actors = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "cleanup reporting task".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || 0_u32),
                end: async move |_: u32| {
                    if mode == 1 {
                        panic!("cleanup sentinel");
                    }

                    if mode == 0 {
                        Err(((), String::from("cleanup sentinel")))
                    } else {
                        Ok(())
                    }
                },
                hooks: AktorHooks {
                    intervals: vec![],
                    before_each: move |_: &mut u32, _| {
                        std::hint::black_box(&held);
                    },
                    after_each: (),
                },
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });
        actors.ready().await.unwrap();

        let report = actors.stop().await;
        let message = if mode == 2 {
            "panic payload drop failed"
        } else {
            "cleanup sentinel"
        };
        assert_eq!(report.to_string().matches(message).count(), 1, "{report}");
        assert_eq!(
            report.actors[0].retirement(),
            Some(if mode == 1 {
                AktorRetirement::Terminated
            } else {
                AktorRetirement::Released
            })
        );
    }
}

#[cfg(feature = "bevy")]
#[tokio::test]
async fn mixed_cleanup() {
    let pool = bevy_tasks::TaskPoolBuilder::new().num_threads(1).build();

    for bevy_first in [false, true] {
        let ended = Arc::new(Mutex::new(false));
        let end = ended.clone();
        let tokio = AktorNew {
            name: "Tokio counter".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || 0_u32),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        };

        let bevy = AktorNew {
            name: "Bevy counter".into(),
            kind: AktorKind::BevyTask(&pool),
            start: AktorInit::Ready(async || 0_u32),
            end: async move |_: u32| {
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
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        };

        let report = if bevy_first {
            let actors = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: (bevy, tokio),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });

            actors.ready().await.unwrap();
            actors.stop().await
        } else {
            let actors = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: (tokio, bevy),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });

            actors.ready().await.unwrap();
            actors.stop().await
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
                name: "task admission".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::MIN,
                stuck: AktorStuck::Kill,
            };

            #[cfg(feature = "bevy")]
            let actors = if mode == 1 {
                aktor_start(AktorSetup {
                    stuck: AktorGroupStuck::KeepRunning,
                    actors: AktorNew {
                        name: setup.name,
                        kind: AktorKind::BevyTask(&pool),
                        start: setup.start,
                        end: (),
                        hooks: AktorHooks::NONE,
                        capacity: setup.capacity,
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                })
            } else {
                aktor_start(AktorSetup {
                    stuck: AktorGroupStuck::KeepRunning,
                    actors: setup,
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                })
            };

            #[cfg(not(feature = "bevy"))]
            let mut actors = {
                let _ = mode;
                aktor_start(AktorSetup {
                    stuck: AktorGroupStuck::KeepRunning,
                    actors: setup,
                    shutdown: |_| {},
                    grace: Duration::from_secs(5),
                })
            };

            actors.ready().await.unwrap();

            let entered = Arc::new(Notify::new());
            let first_gate = Arc::new(Notify::new());
            let second_gate = Arc::new(Notify::new());
            let first = task_wait(actors.handles(), entered.clone(), first_gate.clone())
                .send()
                .await
                .unwrap();

            entered.notified().await;

            let mut second = task_wait(actors.handles(), entered.clone(), second_gate.clone())
                .send()
                .await
                .unwrap();
            let mut request = task_add(actors.handles(), 1).into_future();

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

            let next = request.send().await.unwrap();

            second_gate.notify_one();

            assert_eq!(first.await, Ok(1));
            assert_eq!(next.await, Ok(Ok(3)));
            assert!(
                count.0.load(std::sync::atomic::Ordering::Relaxed) > 0,
                "try_take replaced the task reply waker"
            );
            assert_eq!(second.try_take(), Some(Ok(2)));
            assert_eq!(second.try_take(), None);

            let held = task_wait(actors.handles(), entered.clone(), first_gate.clone())
                .send()
                .await
                .unwrap();

            entered.notified().await;

            let queued = task_add(actors.handles(), 1).send().await.unwrap();
            let mut waiting = task_add(actors.handles(), 1).into_future();
            assert!((&mut waiting).now_or_never().is_none());

            first_gate.notify_one();
            assert_eq!(waiting.await, Ok(Ok(6)));
            assert_eq!(held.await, Ok(4));
            assert_eq!(queued.await, Ok(Ok(5)));

            let held = task_wait(actors.handles(), entered.clone(), first_gate.clone())
                .send()
                .await
                .unwrap();

            entered.notified().await;

            let queued = task_add(actors.handles(), 1).send().await.unwrap();
            let mut waiting = task_add(actors.handles(), 1).into_future();

            assert!((&mut waiting).now_or_never().is_none());
            actors.killswitch().stop();
            drop(waiting);

            first_gate.notify_one();
            assert_eq!(held.await, Ok(7));
            assert_eq!(queued.await, Ok(Ok(8)));
            assert!(!actors.stop().await.failed());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn close_full_queue() {
    for assigned in [false, true] {
        for mode in 0..4 {
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let actors = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "parked admission".into(),
                    kind: AktorKind::TokioTask,
                    start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
                    end: (),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::MIN,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            actors.ready().await.unwrap();

            let first = task_wait(actors.handles(), entered.clone(), release.clone())
                .send()
                .await
                .unwrap();

            entered.notified().await;

            let second = task_wait(actors.handles(), entered.clone(), release.clone())
                .send()
                .await
                .unwrap();
            let request = task_add(actors.handles(), 100);
            let mut waiting: Pin<
                Box<dyn Future<Output = Result<(), aktor::AktorDeliveryError>> + '_>,
            > = match mode {
                0 => Box::pin(async { request.await.map(drop) }),
                1 => Box::pin(async { request.send().await.map(drop) }),
                2 => Box::pin(task_echo(actors.handles(), ()).cast()),
                _ => Box::pin(async {
                    request
                        .timeout(Duration::from_secs(60))
                        .await
                        .unwrap()
                        .map(drop)
                }),
            };

            assert!(waiting.as_mut().now_or_never().is_none());

            if assigned {
                release.notify_one();
                entered.notified().await;
            }

            actors.killswitch().stop();

            if !assigned {
                release.notify_one();
                entered.notified().await;
            }

            release.notify_one();

            let report = tokio::time::timeout(Duration::from_secs(1), actors.completion())
                .await
                .expect("unadmitted waiter kept shutdown alive")
                .unwrap();

            assert!(!report.failed(), "{report}");
            assert_eq!(
                waiting.await.unwrap_err().outcome,
                aktor::AktorDeliveryOutcome::NotAdmitted
            );
            assert_eq!(first.await, Ok(1));
            assert_eq!(second.await, Ok(2));
        }
    }
}

#[tokio::test]
async fn consumed() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "consumed task".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let mut request = task_echo(actors.handles(), ()).into_future();
    (&mut request).await.unwrap();

    let reused = std::panic::AssertUnwindSafe(async {
        drop(request.send().await.unwrap());
    })
    .catch_unwind()
    .await;

    assert!(reused.is_err(), "consumed task returned another reply");

    assert!(!actors.stop().await.failed());
}

#[aktor]
async fn task_void(counter: &mut Counter, by: u32) {
    *counter.0.get_mut() += by;
}

#[tokio::test]
async fn send_futures_for_task_calls() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "sendable task".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let handle = actors.handles().clone();
    let kill = actors.killswitch();

    tokio::spawn(async move {
        task_void(&handle, 1).cast().await.unwrap();
        let _ = task_add(&handle, 2).send().await.unwrap().await;
        let _ = handle.run(task_add(&handle, 3)).await;
        let _ = handle.run(task_void(&handle, 4).cast()).await;
        let _ = kill.run(task_void(&handle, 5).cast()).await;
    })
    .await
    .unwrap();

    assert_eq!(task_add(actors.handles(), 1).await, Ok(Ok(16)));
    assert!(!actors.stop().await.failed());
}

#[tokio::test]
async fn admit_and_settle_on_task() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "task admit settle".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(4).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let handle = actors.handles().clone();
    tokio::spawn(async move {
        let reply = task_add(&handle, 2).send().await.unwrap();
        assert_eq!(reply.await, Ok(Ok(2)));
        assert_eq!(task_add(&handle, 3).await, Ok(Ok(5)));
    })
    .await
    .unwrap();

    let handle = actors.handles().clone();
    assert!(!actors.stop().await.failed());
    assert!(matches!(
        task_add(&handle, 1).send().await,
        Err(aktor::AktorDeliveryError {
            outcome: aktor::message::AktorDeliveryOutcome::NotAdmitted,
            ..
        })
    ));
    assert!(matches!(
        task_add(&handle, 1).await,
        Err(aktor::AktorDeliveryError {
            outcome: aktor::message::AktorDeliveryOutcome::NotAdmitted,
            ..
        })
    ));
}

#[aktor]
async fn task_spawn_job(jobs: &mut Vec<AktorOwnerTask<u32>>, value: u32) {
    assert!(
        std::panic::catch_unwind(
            || aktor_spawn_local(async {}, aktor::AktorJob::FinishBeforeEnd).unwrap()
        )
        .is_err()
    );
    let mut stopping = aktor_stopping();
    let task = aktor_spawn(
        async move {
            stopping.stopped().await;
            Ok::<_, ((), String)>(value)
        },
        aktor::AktorJob::FinishBeforeEnd,
    )
    .unwrap();
    jobs.push(task);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_work_on_task() {
    let finished = Arc::new(Mutex::new(Vec::new()));
    let saved = finished.clone();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "task spawned".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(async || Vec::<AktorOwnerTask<u32>>::new()),
            end: async move |jobs: Vec<AktorOwnerTask<u32>>| {
                for job in jobs {
                    if let Ok(value) = job.await {
                        saved.lock().unwrap().push(value);
                    }
                }
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(4).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    task_spawn_job(actors.handles(), 10).await.unwrap();
    task_spawn_job(actors.handles(), 20).await.unwrap();

    let report = actors.stop().await;
    assert!(!report.failed(), "{report}");
    assert_eq!(*finished.lock().unwrap(), vec![10, 20]);
}

#[aktor]
async fn task_retire_self(
    counter: &mut Counter,
    me: AktorTask<Counter>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
) -> Result<u32, u32> {
    *counter.0.get_mut() += 1;
    entered.notify_one();
    release.notified().await;
    me.retire();
    assert!(me.is_closed());
    Err(85)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retire() {
    let ended = Arc::new(Mutex::new(Vec::new()));
    let first_end = ended.clone();
    let second_end = ended.clone();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: (
            AktorNew {
                name: "first".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
                end: async move |_: Counter| {
                    first_end.lock().unwrap().push("first");
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "second".into(),
                kind: AktorKind::TokioTask,
                start: AktorInit::Ready(async || Counter(std::cell::RefCell::new(0))),
                end: async move |_: Counter| {
                    second_end.lock().unwrap().push("second");
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let (first, second) = actors.handles();

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let retiring = task_retire_self(first, first.clone(), entered.clone(), release.clone())
        .send()
        .await
        .unwrap();
    entered.notified().await;

    let queued = task_add(first, 1).send().await.unwrap();
    release.notify_one();

    assert_eq!(retiring.await, Ok(Err(85)));
    assert!(first.is_closed());
    assert!(matches!(
        queued.await,
        Err(aktor::AktorDeliveryError {
            outcome: aktor::message::AktorDeliveryOutcome::Discarded,
            ..
        })
    ));
    assert!(matches!(
        task_add(first, 1).send().await,
        Err(aktor::AktorDeliveryError {
            outcome: aktor::message::AktorDeliveryOutcome::NotAdmitted,
            ..
        })
    ));

    let report = actors.completion().wait().await.unwrap();
    assert!(!report.failed(), "{report}");
    assert!(
        matches!(report.stop_cause, aktor::StopCause::Retired(id) if report.actor(id).unwrap().name == "first")
    );
    assert!(second.is_closed());
    assert_eq!(ended.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_work_can_retire_task_actor() {
    let (trigger, wait_trigger) = oneshot::channel::<()>();
    let ended = Arc::new(Mutex::new(None));
    let saved = ended.clone();
    let stopped_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopped_out = stopped_flag.clone();

    let started = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "task loop".into(),
            kind: AktorKind::TokioTask,
            start: AktorInit::Ready(move || {
                let mut stopping = aktor_stopping();
                let _watcher = aktor_spawn(
                    async move {
                        stopping.stopped().await;
                        stopped_flag.store(true, std::sync::atomic::Ordering::Release);
                    },
                    aktor::AktorJob::FinishBeforeEnd,
                )
                .unwrap();
                let loop_task = aktor_spawn(
                    async move {
                        let _ = wait_trigger.await;
                        aktor_retire();
                        Ok::<_, ((), String)>("done")
                    },
                    aktor::AktorJob::FinishBeforeEnd,
                )
                .unwrap();
                core::future::ready(loop_task)
            }),
            end: async move |loop_task: AktorOwnerTask<&'static str>| {
                *saved.lock().unwrap() = loop_task.await.ok();
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    started.ready().await.unwrap();

    trigger.send(()).unwrap();
    let report = tokio::time::timeout(Duration::from_secs(2), started.completion().wait())
        .await
        .expect("aktor_retire did not stop the task actor")
        .unwrap();
    assert!(!report.failed(), "{report}");
    assert_eq!(*ended.lock().unwrap(), Some("done"));
    assert!(stopped_out.load(std::sync::atomic::Ordering::Acquire));
}
