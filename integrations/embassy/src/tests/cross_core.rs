use super::*;
use aktor::cross_core;
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
    stopping_startup().await;
    startup_origin().await;
    reply_wake().await;

    let stored = Rc::new(RefCell::new(Vec::new()));
    let cleaned = stored.clone();
    let intervals = Rc::new(Cell::new(0));
    let ticks = intervals.clone();
    let interval_ready = Rc::new(Signal::<NoopRawMutex, ()>::new());
    let ticked = interval_ready.clone();
    let before = Rc::new(Cell::new(0));
    let log = before.clone();
    let setup = AktorNew {
        name: AktorName::new("shared sensor"),
        role: AktorNoRole,
        kind: AktorKind::EmbassyCrossCore(move |future| {
            spawner.spawn(
                setup_driver(future).map_err(|_| AktorSetupError::new("no shared driver slot"))?,
            );
            Ok(())
        }),
        closures: AktorClosures {
            start: async || {
                Ok(Sensor {
                    readings: Rc::new(RefCell::new(Vec::new())),
                })
            },

            end: Some(
                (async move |sensor: Sensor| {
                    *cleaned.borrow_mut() = sensor.readings.borrow().clone();
                    Ok(())
                })
                .into(),
            ),
            intervals: vec![AktorInterval {
                every: Duration::from_millis(1),
                run: (async move |_: &mut Sensor| {
                    ticks.set(ticks.get() + 1);
                    ticked.signal(());
                })
                .into(),
            }],
            before_each: Some(
                (move |_: &mut Sensor, _: operation::Operation| log.set(log.get() + 1)).into(),
            ),
            after_each: None,
        },
        options: AktorNewOptions { capacity: 1 },
    };

    let actors = aktor_start(AktorSetup {
        actors: setup,
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    interval_ready.wait().await;
    assert!(intervals.get() > 0);
    assert!(record(&actors.handles, 0).await.is_err());
    assert_eq!(
        record_many(&actors.handles, vec![4, 5])
            .await
            .unwrap_report(),
        2
    );

    let entered = Arc::new(Signal::new());
    let released = Arc::new(Signal::new());
    let prepared: SharedGate = Arc::new(Signal::new());
    let caller = actors.handles.clone();
    let in_call = entered.clone();
    let release = released.clone();
    let queued = prepared.clone();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = finished.clone();
    let caller = thread::spawn(move || {
        run(async move {
            let mut held = hold_shared(&caller, in_call, release).send().await;
            let timeout = held.timeout(Duration::from_millis(1)).await.unwrap_err();

            assert!(timeout.admitted);
            drop(held);

            let ordinary = record(&caller, 2).send().await;

            assert!(record(&caller, 3).send().now_or_never().is_none());

            let (input, mut output) = record::latest(&caller);

            input.send(9);
            input.send(10);
            input.send(11);
            queued.signal(());
            assert!(output.next().await.unwrap().is_ok());
            assert!(ordinary.await.is_ok());
            assert!(output.next().await.is_none());
            drop(input);
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        })
    });

    entered.wait().await;
    prepared.wait().await;
    actors.killswitch().stop();
    released.signal(());

    let report = actors.shutdown().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(
        report.actors[0].kind,
        Some(AktorExecution::EmbassyCrossCore)
    );

    while !finished.load(std::sync::atomic::Ordering::SeqCst) {
        Timer::after_millis(1).await;
    }

    caller.join().unwrap();
    assert_eq!(*stored.borrow(), [4, 5, 1, 11, 2]);
    assert!(before.get() >= 6);

    let (handle, owner) = cross_core::channel::<Sensor>(1).unwrap();
    let (input, mut output) = record::latest(&handle);

    assert!(output.next().now_or_never().is_none());
    drop(owner);
    assert!(handle.completion().await.is_err());
    assert!(support::panics(output.next()).await);
    drop(input);
    assert!(handle.downgrade().upgrade().is_none());

    let (handle, owner) = cross_core::channel::<Sensor>(1).unwrap();
    let mut request = record::request(&handle, 85);

    assert!(support::poll(&mut request).is_pending());

    let reply = request.send().await;
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

    assert_eq!(reply.await.unwrap_report(), 1);
    assert!(completion.await.is_ok());

    let actors = aktor_start(AktorSetup {
        actors: aktor_setups! {
            shared: shared_sensor_setup(spawner),
            local: sensor_setup(spawner),
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    assert_eq!(record(&actors.handles.local, 4).await.unwrap_report(), 1);

    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callers: Vec<_> = (0..4)
        .map(|caller| {
            let handle = actors.handles.shared.clone();
            let done = done.clone();

            thread::spawn(move || {
                run(async move {
                    for _ in 0..8 {
                        record(&handle, caller + 1).await.unwrap_report();
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
        record_many(&actors.handles.shared, vec![])
            .await
            .unwrap_report(),
        32
    );

    let shared = actors.handles.shared.clone();
    let (reply, owner) = (record(&shared, 85).send().await, actors.shutdown());
    let report = owner.await;

    assert!(!report.failed());
    assert_eq!(reply.await.unwrap_report(), 33);
    cancellation().await;
}

async fn shutdown_timeout() {
    fn expired(future: impl Future<Output = Result<usize, AktorTimeoutError>>) -> bool {
        let mut timeout = Box::pin(future);

        assert!(support::poll(&mut timeout).is_pending());

        match support::poll(&mut timeout) {
            Poll::Ready(Err(error)) => {
                assert!(error.admitted);
                true
            }
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => panic!("held operation completed before release"),
        }
    }

    let mut timeouts = Vec::new();

    for (managed, stop_group) in [(false, false), (true, false), (true, true)] {
        for reply_only in [false, true] {
            let group = aktor::embassy::AktorGroup::new();
            let readings = Rc::new(RefCell::new(Vec::new()));
            let stored = readings.clone();
            let (handle, mut owner) = cross_core::channel::<Sensor>(1).unwrap();

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
            let mut request = hold_shared(&handle, entered.clone(), released.clone());

            assert!(support::poll(&mut request).is_pending());
            assert!(support::poll(&mut running).is_pending());
            entered.wait().await;

            if stop_group {
                group.killswitch().stop();
            } else {
                handle.shutdown();
            }

            let timeout = if reply_only {
                expired(request.send().await.timeout(Duration::ZERO))
            } else {
                expired(request.timeout(Duration::ZERO))
            };

            let completed = handle.shutdown();

            released.signal(());
            running.await.unwrap();
            completed.await.unwrap();

            let waiting = record(&handle, 0).send();
            if managed {
                assert!(waiting.now_or_never().is_none());
            } else {
                assert!(
                    std::panic::AssertUnwindSafe(waiting)
                        .catch_unwind()
                        .await
                        .is_err()
                );
            }

            timeouts.push(timeout);
            assert_eq!(*readings.borrow(), [1]);
        }
    }

    assert_eq!(timeouts, [true, true, true, true, false, false]);
}

async fn interval_delay() {
    let ticks = Rc::new(Cell::new(0));
    let tick = ticks.clone();
    let every = Duration::from_millis(100);
    let (handle, mut owner) = cross_core::channel::<Sensor>(1).unwrap();

    owner
        .set_intervals(vec![(
            every,
            (async move |_: &mut Sensor| tick.set(tick.get() + 1)).into(),
        )])
        .unwrap();

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
    let mut request = Box::pin(hold_shared(&handle, entered.clone(), released.clone()));

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
        name: AktorName::new("cancelled shared sensor"),
        role: AktorNoRole,
        kind: AktorKind::EmbassyCrossCore(move |future| {
            *stored.borrow_mut() = Some(future);
            Ok(())
        }),
        closures: AktorClosures {
            start: async || {
                Ok(Sensor {
                    readings: Rc::new(RefCell::new(Vec::new())),
                })
            },

            end: Some(
                (async move |_: Sensor| {
                    cleanup.set(true);
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

    let startup = aktor_start(AktorSetup {
        actors: setup,
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    });
    let completion = startup.completion();
    let mut startup = Box::pin(startup);
    let actors = core::future::poll_fn(|cx| {
        let result = startup.as_mut().poll(cx);

        if let Some(driver) = driver.borrow_mut().as_mut() {
            assert!(driver.as_mut().poll(cx).is_pending());
        }

        result
    })
    .await
    .unwrap();

    let (input, mut output) = record::latest(&actors.handles);
    let entered = Arc::new(Signal::new());
    let mut reply = hold_shared(&actors.handles, entered.clone(), Arc::new(Signal::new()))
        .send()
        .await;

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

    let report = completion.await;

    assert!(report.failed());
    assert!(!cleaned.get());
    assert!(support::poll(&mut reply).is_pending());

    let mut timeout = Box::pin(reply.timeout(Duration::ZERO));

    assert!(support::poll(&mut timeout).is_pending());
    assert!(support::poll(&mut timeout).is_pending());
    drop(timeout);
    assert!(output.next().now_or_never().is_none());
    drop(input);
}

async fn callbacks() {
    struct Reenter {
        input: record::LatestSender<cross_core::LatestSender<(u32,)>>,
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
        _input: record::LatestSender<cross_core::LatestSender<(u32,)>>,
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

    let (handle, owner) = cross_core::channel::<Sensor>(1).unwrap();
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
    assert_eq!(output.next().await.unwrap().unwrap_report(), 1);

    let complete = handle.shutdown();

    running.await.unwrap();
    complete.await.unwrap();
}

async fn reply_wake() {
    let (handle, owner) = cross_core::channel::<Sensor>(1).unwrap();
    let mut reply = record(&handle, 1).send().await;

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
    assert_eq!(reply.try_take().unwrap().unwrap_report(), 1);
    assert!(reply.try_take().is_none());
    completed.await.unwrap();
}

async fn interval_fairness() {
    let every = Duration::from_millis(1);
    let ticks = Rc::new(Cell::new(0));
    let tick = ticks.clone();
    let (handle, mut owner) = cross_core::channel::<Sensor>(4).unwrap();

    owner
        .set_intervals(vec![(
            every,
            (async move |_: &mut Sensor| tick.set(tick.get() + 1)).into(),
        )])
        .unwrap();

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
                .await,
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
                result.unwrap_report();
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
        reply.await;
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
    let (handle, mut owner) = cross_core::channel::<Sensor>(1).unwrap();

    owner
        .set_intervals(vec![(
            every,
            (async move |_: &mut Sensor| {
                tick.set(tick.get() + 1);
            })
            .into(),
        )])
        .unwrap();

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

async fn stopping_startup() {
    let driver = Rc::new(RefCell::new(None));
    let stored = driver.clone();
    let entered = Rc::new(Signal::<NoopRawMutex, ()>::new());
    let begin = entered.clone();
    let release = Rc::new(Signal::<NoopRawMutex, ()>::new());
    let gate = release.clone();
    let cleaned = Rc::new(Cell::new(0));
    let end = cleaned.clone();
    let startup = aktor_start(AktorSetup {
        actors: AktorNew {
            name: AktorName::new("stopping shared setup"),
            role: AktorNoRole,
            kind: AktorKind::EmbassyCrossCore(move |future| {
                *stored.borrow_mut() = Some(future);
                Ok(())
            }),
            closures: AktorClosures {
                start: async move || {
                    begin.signal(());
                    gate.wait().await;
                    Ok(0_u32)
                },
                end: Some(
                    (async move |_: u32| {
                        end.set(end.get() + 1);
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
            shutdown_grace: Duration::from_secs(5),
        },
    });

    let kill = startup.killswitch();
    let completion = startup.completion();
    let mut startup = Box::pin(startup);

    assert!(support::poll(&mut startup).is_pending());
    assert!(support::poll(driver.borrow_mut().as_mut().unwrap()).is_pending());
    entered.wait().await;
    kill.stop();
    release.signal(());

    let result = poll_fn(|cx| {
        if let Some(driver) = driver.borrow_mut().as_mut() {
            let _result = driver.as_mut().poll(cx);
        }

        startup.as_mut().poll(cx)
    })
    .await;

    assert!(result.is_err(), "stopping shared setup returned a handle");

    let report = completion.await;

    assert!(!report.timed_out, "{report}");
    assert_eq!(cleaned.get(), 1);
}

async fn startup_origin() {
    for runtime_failure in [false, true] {
        let mut group = embassy::AktorGroup::with_grace(Duration::from_secs(5));
        let notification = Rc::new(RefCell::new(None));
        let observed = notification.clone();

        group
            .on_shutdown(move |report| *observed.borrow_mut() = Some(report))
            .unwrap();

        let mut driver = Box::pin(group.listen().unwrap());
        let release = Rc::new(Signal::<NoopRawMutex, ()>::new());
        let gate = release.clone();
        let mut opening = Box::pin(aktor_start_in(
            &group,
            AktorNew {
                name: AktorName::new("bad settings"),
                role: AktorNoRole,
                kind: AktorKind::EmbassyCrossCore(|_| Ok(())),
                closures: AktorClosures {
                    start: async move || {
                        gate.wait().await;
                        Err::<u32, _>(AktorSetupError::new("invalid settings"))
                    },
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
            },
        ));

        assert!(support::poll(&mut opening).is_pending());
        assert!(support::poll(&mut driver).is_pending());

        if runtime_failure {
            group.killswitch().fail(aktor::ActorFailure {
                actor: "existing actor".into(),
                kind: Some(AktorExecution::EmbassyCrossCore),
                phase: "operation".into(),
                message: "earlier runtime failure".into(),
            });
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

        assert_eq!(notification.startup, !runtime_failure, "{notification}");
        assert_eq!(error.report.unwrap().startup, notification.startup);
    }
}
