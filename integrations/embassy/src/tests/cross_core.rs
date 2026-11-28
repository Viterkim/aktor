use super::*;
use aktor::cross_core;
use core::future::IntoFuture;
use core::num::NonZeroUsize;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use std::{
    future::poll_fn,
    sync::Arc,
    task::Wake,
    thread,
    time::{Duration, Instant},
};

type SharedGate = Arc<Signal<CriticalSectionRawMutex, ()>>;

#[aktor]
async fn hold_shared(sensor: &mut Sensor, entered: SharedGate, release: SharedGate) -> usize {
    let readings = sensor.readings.clone();

    entered.signal(());
    release.wait().await;
    readings.borrow_mut().push(1);
    readings.borrow().len()
}

struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn run<F: Future>(future: F) -> F::Output {
    let deadline = Instant::now() + Duration::from_secs(5);
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = core::pin::pin!(future);

    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
            return result;
        }

        assert!(Instant::now() < deadline, "shared caller lost its wake");
        thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
    }
}

pub async fn check(spawner: Spawner) {
    callbacks().await;
    shutdown_timeout().await;
    interval_delay().await;
    interval_fairness().await;
    stopping_startup(true).await;
    stopping_startup(false).await;
    startup_origin().await;
    reply_wake().await;
    stuck_call_end(spawner).await;

    let stored = Rc::new(RefCell::new(Vec::new()));
    let cleaned = stored.clone();
    let intervals = Rc::new(Cell::new(0));
    let ticks = intervals.clone();
    let interval_ready = Rc::new(Signal::<NoopRawMutex, ()>::new());
    let ticked = interval_ready.clone();
    let before = Rc::new(Cell::new(0));
    let log = before.clone();
    let setup = AktorNew {
        name: "shared sensor".into(),
        kind: AktorKind::EmbassyCrossCore(move |future| {
            spawner.spawn(setup_driver(future).er(())?);
            Ok::<(), (crate::SpawnDriverErr, String)>(())
        }),
        start: aktor::AktorInit::Ready(async || Sensor {
            readings: Rc::new(RefCell::new(Vec::new())),
        }),
        end: async move |sensor: Sensor| {
            *cleaned.borrow_mut() = sensor.readings.borrow().clone();
        },
        hooks: AktorHooks {
            intervals: vec![
                AktorInterval::new(Duration::from_millis(1), async move |_: &mut Sensor| {
                    ticks.set(ticks.get() + 1);
                    ticked.signal(());
                })
                .unwrap(),
            ],
            before_each: move |_: &mut Sensor, _: operation::Operation| log.set(log.get() + 1),
            after_each: (),
        },
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    };

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: setup,
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    interval_ready.wait().await;
    assert!(intervals.get() > 0);
    assert!(record(actors.handles(), 0).await.unwrap().is_err());
    assert_eq!(
        record_many(actors.handles(), vec![4, 5])
            .await
            .unwrap()
            .er_unwrap(),
        2
    );

    let entered = Arc::new(Signal::new());
    let released = Arc::new(Signal::new());
    let prepared: SharedGate = Arc::new(Signal::new());
    let caller = actors.handles().clone();
    let in_call = entered.clone();
    let release = released.clone();
    let queued = prepared.clone();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = finished.clone();
    let caller = thread::spawn(move || {
        run(async move {
            let mut held = hold_shared(&caller, in_call, release).send().await.unwrap();
            let timeout = held.timeout(Duration::from_millis(1)).await.unwrap_err();

            assert!(timeout.admitted);
            drop(held);

            let ordinary = record(&caller, 2).send().await.unwrap();

            assert!(record(&caller, 3).send().now_or_never().is_none());

            let (input, mut output) = record::latest(&caller);

            input.send(9);
            input.send(10);
            input.send(11);
            queued.signal(());
            assert!(output.next().await.unwrap().unwrap().is_ok());
            assert!(ordinary.await.unwrap().is_ok());
            assert!(output.next().await.is_none());
            drop(input);
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        })
    });

    entered.wait().await;
    prepared.wait().await;
    actors.killswitch().stop();
    released.signal(());

    let report = actors.stop().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(report.actors[0].kind, AktorExecution::EmbassyCrossCore);

    while !finished.load(std::sync::atomic::Ordering::SeqCst) {
        Timer::after_millis(1).await;
    }

    caller.join().unwrap();
    assert_eq!(*stored.borrow(), [4, 5, 1, 11, 2]);
    assert!(before.get() >= 6);

    let (handle, owner) = cross_core::channel::<Sensor>(NonZeroUsize::MIN);
    let (input, mut output) = record::latest(&handle);

    assert!(output.next().now_or_never().is_none());
    drop(owner);
    assert!(handle.completion().await.is_err());
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
    drop(input);
    assert!(handle.downgrade().upgrade().is_none());

    let (handle, owner) = cross_core::channel::<Sensor>(NonZeroUsize::MIN);
    let mut request = record(&handle, 85).into_future();

    assert!(support::poll(&mut request).is_pending());

    let reply = request.send().await.unwrap();
    let completion = handle.shutdown();

    owner
        .run_with(
            async || {
                Ok(Sensor {
                    readings: Rc::new(RefCell::new(Vec::new())),
                })
            },
            async |sensor| {
                assert_eq!(*sensor.readings.borrow(), [85]);
                Ok(())
            },
        )
        .await
        .unwrap();

    assert_eq!(reply.await.unwrap().er_unwrap(), 1);
    assert!(completion.await.is_ok());

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: (shared_sensor_setup(spawner), sensor_setup(spawner)),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    assert_eq!(record(&actors.handles().1, 4).await.unwrap().er_unwrap(), 1);

    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callers: Vec<_> = (0..4)
        .map(|caller| {
            let handle = actors.handles().0.clone();
            let done = done.clone();

            thread::spawn(move || {
                run(async move {
                    for _ in 0..8 {
                        record(&handle, caller + 1).await.unwrap().er_unwrap();
                    }

                    done.fetch_add(1, Ordering::SeqCst);
                })
            })
        })
        .collect();

    while done.load(Ordering::SeqCst) != 4 {
        Timer::after_millis(1).await;
    }

    for caller in callers {
        caller.join().unwrap();
    }

    assert_eq!(
        record_many(&actors.handles().0, vec![])
            .await
            .unwrap()
            .er_unwrap(),
        32
    );

    let shared = actors.handles().0.clone();
    let (reply, owner) = (record(&shared, 85).send().await.unwrap(), actors.stop());
    let report = owner.await;

    assert!(!report.failed());
    assert_eq!(reply.await.unwrap().er_unwrap(), 33);
    cancellation().await;
}

async fn shutdown_timeout() {
    fn expired(
        future: impl Future<
            Output = Result<Result<usize, aktor::AktorDeliveryError>, AktorTimeoutError>,
        >,
    ) {
        let mut timeout = Box::pin(future);

        assert!(support::poll(&mut timeout).is_pending());

        let Poll::Ready(Err(_)) = support::poll(&mut timeout) else {
            panic!("held call did not time out during shutdown");
        };
    }

    for (managed, stop_group) in [(false, false), (true, false), (true, true)] {
        for reply_only in [false, true] {
            let group = aktor::embassy::AktorGroup::new();
            let readings = Rc::new(RefCell::new(Vec::new()));
            let stored = readings.clone();
            let (handle, mut owner) = cross_core::channel::<Sensor>(NonZeroUsize::MIN);

            if managed {
                owner.manage("closing sensor".into(), group.killswitch());
            }

            let mut running = Box::pin(owner.run_with(
                async move || Ok(Sensor { readings: stored }),
                async |_| Ok(()),
            ));

            assert!(support::poll(&mut running).is_pending());

            let entered = Arc::new(Signal::new());
            let released = Arc::new(Signal::new());
            let mut request = hold_shared(&handle, entered.clone(), released.clone()).into_future();

            assert!(support::poll(&mut request).is_pending());
            assert!(support::poll(&mut running).is_pending());
            entered.wait().await;

            if stop_group {
                group.killswitch().stop();
            } else {
                handle.shutdown();
            }

            if reply_only {
                expired(request.send().await.unwrap().timeout(Duration::ZERO))
            } else {
                expired(request.timeout(Duration::ZERO))
            }

            let completed = handle.shutdown();

            released.signal(());
            running.await.unwrap();
            completed.await.unwrap();

            let waiting = record(&handle, 0).send();
            assert_eq!(
                waiting.await.err().unwrap().outcome,
                aktor::AktorDeliveryOutcome::NotAdmitted
            );

            assert_eq!(*readings.borrow(), [1]);
        }
    }
}

async fn interval_delay() {
    let ticks = Rc::new(Cell::new(0));
    let tick = ticks.clone();
    let every = Duration::from_millis(100);
    let (handle, mut owner) = cross_core::channel::<Sensor>(NonZeroUsize::MIN);

    owner.set_intervals(vec![
        AktorInterval::new(every, async move |_: &mut Sensor| tick.set(tick.get() + 1)).unwrap(),
    ]);

    let mut running = Box::pin(owner.run_with(
        async || {
            Ok(Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            })
        },
        async |_| Ok(()),
    ));

    assert!(support::poll(&mut running).is_pending());
    Timer::after(embassy_time::Duration::from_millis(100)).await;
    assert!(support::poll(&mut running).is_pending());
    assert_eq!(ticks.get(), 1);

    let entered = Arc::new(Signal::new());
    let released = Arc::new(Signal::new());
    let mut request =
        Box::pin(hold_shared(&handle, entered.clone(), released.clone()).into_future());

    assert!(support::poll(&mut request).is_pending());
    assert!(support::poll(&mut running).is_pending());
    entered.wait().await;
    Timer::after(embassy_time::Duration::from_millis(200)).await;
    released.signal(());
    assert!(support::poll(&mut running).is_pending());
    assert!(support::poll(&mut request).is_ready());
    assert!(support::poll(&mut running).is_pending());
    assert_eq!(ticks.get(), 2);

    let completed = handle.shutdown();

    running.await.unwrap();
    completed.await.unwrap();
}

async fn cancellation() {
    let driver = Rc::new(RefCell::new(None));
    let stored = driver.clone();
    let cleaned = Rc::new(Cell::new(false));
    let cleanup = cleaned.clone();
    let setup = AktorNew {
        name: "cancelled shared sensor".into(),
        kind: AktorKind::EmbassyCrossCore(move |future| {
            *stored.borrow_mut() = Some(future);
            Ok::<(), ((), String)>(())
        }),
        start: aktor::AktorInit::Ready(async || Sensor {
            readings: Rc::new(RefCell::new(Vec::new())),
        }),
        end: async move |_: Sensor| {
            cleanup.set(true);
        },
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: setup,
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    let completion = actors.completion();
    let mut ready = Box::pin(actors.ready());

    core::future::poll_fn(|cx| {
        let result = ready.as_mut().poll(cx);

        if let Some(driver) = driver.borrow_mut().as_mut() {
            assert!(driver.as_mut().poll(cx).is_pending());
        }

        result
    })
    .await
    .unwrap();
    drop(ready);

    let (input, mut output) = record::latest(actors.handles());
    let entered = Arc::new(Signal::new());
    let reply = hold_shared(actors.handles(), entered.clone(), Arc::new(Signal::new()))
        .send()
        .await
        .unwrap();

    assert!(
        driver
            .borrow_mut()
            .as_mut()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    entered.wait().await;
    drop(driver.borrow_mut().take());

    let report = completion.await.unwrap();

    assert!(report.failed());
    assert!(!cleaned.get());
    assert_eq!(
        reply.await.unwrap_err().failure_id,
        Some(report.failures[0].id)
    );
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
    drop(input);
}

async fn callbacks() {
    struct Reenter {
        input: record::Sender<cross_core::LatestSender<(u32,)>>,
        sent: std::sync::atomic::AtomicBool,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if !self.sent.swap(true, std::sync::atomic::Ordering::SeqCst) {
                self.input.send(85);
            }
        }
    }
    struct DropSender {
        _input: record::Sender<cross_core::LatestSender<(u32,)>>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    // This waker tests destruction, including the sender it owns.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for DropSender {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for DropSender {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let (handle, owner) = cross_core::channel::<Sensor>(NonZeroUsize::MIN);
    let (input, mut output) = record::latest(&handle);
    let waker = Waker::from(Arc::new(Reenter {
        input: input.clone(),
        sent: std::sync::atomic::AtomicBool::new(false),
    }));
    let mut running = Box::pin(owner.run_with(
        async || {
            Ok(Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            })
        },
        async |sensor| {
            assert_eq!(*sensor.readings.borrow(), [85]);
            Ok(())
        },
    ));

    assert!(
        running
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let old = Waker::from(Arc::new(DropSender {
        _input: input.clone(),
        dropped: dropped.clone(),
    }));

    assert!(
        core::pin::pin!(output.next())
            .as_mut()
            .poll(&mut Context::from_waker(&old))
            .is_pending()
    );
    drop(old);
    assert!(output.next().now_or_never().is_none());
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    input.send(1);
    assert!(
        running
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(output.next().await.unwrap().unwrap().er_unwrap(), 1);

    let complete = handle.shutdown();

    running.await.unwrap();
    complete.await.unwrap();
}

async fn reply_wake() {
    let (handle, owner) = cross_core::channel::<Sensor>(NonZeroUsize::MIN);
    let mut reply = record(&handle, 1).send().await.unwrap();

    assert!(reply.try_take().is_none());

    let count = std::sync::Arc::new(CountWake(core::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());

    assert!(
        Pin::new(&mut reply)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(reply.try_take().is_none());

    let completed = handle.shutdown();

    owner
        .run_with(
            async || {
                Ok(Sensor {
                    readings: Rc::new(RefCell::new(Vec::new())),
                })
            },
            async |_| Ok(()),
        )
        .await
        .unwrap();

    assert!(
        count.0.load(Ordering::Relaxed) > 0,
        "try_take replaced the shared reply waker"
    );
    assert_eq!(reply.try_take().unwrap().unwrap().er_unwrap(), 1);
    assert!(reply.try_take().is_none());
    completed.await.unwrap();
}

async fn interval_fairness() {
    let every = Duration::from_millis(1);
    let ticks = Rc::new(Cell::new(0));
    let tick = ticks.clone();
    let (handle, mut owner) = cross_core::channel::<Sensor>(NonZeroUsize::new(4).unwrap());

    owner.set_intervals(vec![
        AktorInterval::new(every, async move |_: &mut Sensor| tick.set(tick.get() + 1)).unwrap(),
    ]);

    let entered = Arc::new(Signal::new());
    let release = Arc::new(Signal::new());
    let mut running = Box::pin(owner.run_with(
        async || {
            Ok(Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            })
        },
        async |_| Ok(()),
    ));

    assert!(support::poll(&mut running).is_pending());

    let mut replies = Vec::new();

    for _ in 0..4 {
        replies.push(
            hold_shared(&handle, entered.clone(), release.clone())
                .send()
                .await
                .unwrap(),
        );
    }

    assert!(support::poll(&mut running).is_pending());

    let (input, mut results) = record(&handle, 2).latest();
    let mut ran_latest = false;

    for index in 0..4 {
        let mut began = Box::pin(entered.wait());

        poll_fn(|cx| {
            assert!(running.as_mut().poll(cx).is_pending());
            began.as_mut().poll(cx)
        })
        .await;
        Timer::after(embassy_time::Duration::from_millis(2)).await;
        release.signal(());

        for _ in 0..4 {
            assert!(support::poll(&mut running).is_pending());

            if let Poll::Ready(Some(result)) = support::poll(Box::pin(results.next())) {
                result.unwrap().er_unwrap();
                ran_latest = index < 3;
                break;
            }
        }

        if ran_latest && ticks.get() > 0 {
            break;
        }
    }

    let completed = handle.shutdown();

    poll_fn(|cx| {
        release.signal(());
        running.as_mut().poll(cx)
    })
    .await
    .unwrap();

    for reply in replies {
        reply.await.unwrap();
    }

    drop(input);
    completed.await.unwrap();
    assert!(
        ran_latest,
        "intervals starved latest until the ordinary queue drained"
    );
    assert!(ticks.get() > 0);

    let ticks = Rc::new(Cell::new(0));
    let tick = ticks.clone();
    let (handle, mut owner) = cross_core::channel::<Sensor>(NonZeroUsize::MIN);

    owner.set_intervals(vec![
        AktorInterval::new(every, async move |_: &mut Sensor| {
            tick.set(tick.get() + 1);
        })
        .unwrap(),
    ]);

    let mut running = Box::pin(owner.run_with(
        async || {
            Ok(Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            })
        },
        async |_| Ok(()),
    ));

    assert!(support::poll(&mut running).is_pending());
    Timer::after(embassy_time::Duration::from_millis(2)).await;

    let (input, _results) = record::latest(&handle);

    for value in 0..3 {
        input.send(value);
        assert!(support::poll(&mut running).is_pending());
    }

    assert!(
        ticks.get() > 0,
        "continuous latest work starved the due interval"
    );
    drop(handle.shutdown());
    running.await.unwrap();
}

async fn stopping_startup(finish: bool) {
    let driver = Rc::new(RefCell::new(None));
    let stored = driver.clone();
    let entered = Rc::new(Signal::<NoopRawMutex, ()>::new());
    let begin = entered.clone();
    let release = Rc::new(Signal::<NoopRawMutex, ()>::new());
    let gate = release.clone();
    let cleaned = Rc::new(Cell::new(0));
    let end = cleaned.clone();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "stopping shared setup".into(),
            kind: AktorKind::EmbassyCrossCore(move |future| {
                *stored.borrow_mut() = Some(future);
                Ok::<(), ((), String)>(())
            }),
            start: aktor::AktorInit::Ready(async move || {
                begin.signal(());
                gate.wait().await;
                0_u32
            }),
            end: async move |_: u32| {
                end.set(end.get() + 1);
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_millis(20),
    });

    let kill = started.killswitch();
    let completion = started.completion();
    let mut ready = Box::pin(started.ready());

    assert!(support::poll(&mut ready).is_pending());
    assert!(support::poll(driver.borrow_mut().as_mut().unwrap()).is_pending());
    entered.wait().await;
    kill.stop();
    if finish {
        release.signal(());
    }

    let result = poll_fn(|cx| {
        if let Some(driver) = driver.borrow_mut().as_mut() {
            let _result = driver.as_mut().poll(cx);
        }

        ready.as_mut().poll(cx)
    })
    .await;

    assert!(result.is_err(), "stopping startup came back ready");

    let report = completion.await.unwrap();

    assert!(report.released(), "{report}");
    assert_eq!(report.ran_past_grace(), !finish);
    assert_eq!(cleaned.get(), u32::from(finish));
}

async fn startup_origin() {
    for runtime_failure in [false, true] {
        let mut group = embassy::AktorGroup::with_grace(Duration::from_secs(5));
        let notification = Rc::new(RefCell::new(None));
        let observed = notification.clone();

        group
            .on_shutdown(move |report| *observed.borrow_mut() = Some(report))
            .unwrap();

        let mut driver = Box::pin(group.listen(async |_| ()).unwrap());
        let release = Rc::new(Signal::<NoopRawMutex, ()>::new());
        let gate = release.clone();
        let spawned = group.spawn(AktorNew {
            name: "bad settings".into(),
            kind: AktorKind::EmbassyCrossCore(|_| Ok::<(), ((), String)>(())),
            start: aktor::AktorInit::Try(async move || {
                gate.wait().await;
                Err::<u32, _>(((), String::from("invalid settings")))
            }),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        });
        let mut opening = Box::pin(spawned.ready());

        assert!(support::poll(&mut opening).is_pending());
        assert!(support::poll(&mut driver).is_pending());

        if runtime_failure {
            group.killswitch().fail(aktor::AktorFailureInput::actor(
                "existing actor".into(),
                AktorExecution::EmbassyCrossCore,
                aktor::AktorPhase::Runner,
                "earlier runtime failure".into(),
            ));
        }

        release.signal(());

        let error = poll_fn(|cx| {
            let _finished = driver.as_mut().poll(cx);
            opening.as_mut().poll(cx)
        })
        .await
        .err()
        .unwrap();
        let notification = notification.borrow_mut().take().unwrap();

        assert_eq!(
            matches!(notification.stop_cause, aktor::StopCause::StartupFailure(_)),
            !runtime_failure,
            "{notification}"
        );
        assert_eq!(
            matches!(
                error
                    .0
                    .startup()
                    .unwrap()
                    .report
                    .as_ref()
                    .unwrap()
                    .stop_cause,
                aktor::StopCause::StartupFailure(_)
            ),
            matches!(notification.stop_cause, aktor::StopCause::StartupFailure(_))
        );
    }
}

// A call that never finishes gets cut after the grace, and `end` still runs.
async fn stuck_call_end(spawner: Spawner) {
    let ended = Rc::new(Cell::new(false));
    let cleaned = ended.clone();
    let setup = AktorNew {
        name: "stuck sensor".into(),
        kind: AktorKind::EmbassyCrossCore(move |future| {
            spawner.spawn(setup_driver(future).er(())?);
            Ok::<(), (crate::SpawnDriverErr, String)>(())
        }),
        start: aktor::AktorInit::Ready(async || Sensor {
            readings: Rc::new(RefCell::new(Vec::new())),
        }),
        end: async move |_: Sensor| {
            cleaned.set(true);
        },
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    };

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: setup,
        shutdown: |_| {},
        grace: Duration::from_millis(10),
    });
    actors.ready().await.unwrap();

    let entered = Arc::new(Signal::new());
    let never: SharedGate = Arc::new(Signal::new());
    let caller = actors.handles().clone();
    let in_call = entered.clone();
    let caller = thread::spawn(move || {
        run(async move {
            let mut held = hold_shared(&caller, in_call, never).send().await.unwrap();
            held.timeout(Duration::from_secs(3))
                .await
                .unwrap()
                .unwrap_err()
        })
    });

    entered.wait().await;
    let report = actors.stop().await;

    assert!(
        ended.get(),
        "end didn't run after the stuck call was cut: {report}"
    );
    assert!(
        report.ran_past_grace(),
        "the cut call should show in the report: {report}"
    );

    let delivery = caller.join().unwrap();
    assert_eq!(
        delivery.outcome,
        aktor::AktorDeliveryOutcome::OutcomeUnknown
    );
}
