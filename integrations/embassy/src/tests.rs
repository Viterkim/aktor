extern crate std;

use super::*;
use aktor::{
    ShutdownReport,
    message::{LocalFuture, TrySendError},
};
use alloc::{boxed::Box, vec};
use core::{
    cell::Cell,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, Waker},
};
use embassy_executor::{Executor, Spawner};
use futures_util::{FutureExt, future::select};

mod cross_core;
#[path = "../../../aktor/tests/support/mod.rs"]
pub mod support;

static DONE: AtomicBool = AtomicBool::new(false);

type Gate = Rc<Signal<NoopRawMutex, ()>>;
type Owner = embassy::Owner<Sensor, 2, &'static str>;

#[embassy_executor::task(pool_size = 2)]
async fn run_owner(
    owner: Owner,
    readings: Rc<RefCell<Vec<u32>>>,
    cleaned: Rc<Cell<bool>>,
    cancel: Option<Gate>,
) {
    let run = owner.run_with(
        async move || {
            let state = Sensor { readings };

            Timer::after_millis(1).await;

            Ok(state)
        },
        async move |state| {
            Timer::after_millis(1).await;
            cleaned.set(true);
            drop(state);

            Ok(())
        },
    );

    if let Some(cancel) = cancel {
        let _result = select(Box::pin(run), Box::pin(cancel.wait())).await;
    } else {
        run.await.unwrap();
    }
}

#[embassy_executor::task]
async fn exercise(spawner: Spawner) {
    cross_core::check(spawner).await;
    latest_callbacks().await;
    driver_cancellation().await;
    setups(spawner).await;

    for close_first in [false, true] {
        let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();
        let mut request = record::request(&sensor, 1);

        assert!(
            Pin::new(&mut request)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );

        if close_first {
            drop(sensor.shutdown());
        }

        let mut reply = request.try_send().unwrap();

        assert!(reply.try_take().is_none());

        let count = std::sync::Arc::new(CountWake(core::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());

        assert!(
            Pin::new(&mut reply)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(reply.try_take().is_none());

        let completion = sensor.shutdown();

        owner
            .run(
                Sensor {
                    readings: Rc::new(RefCell::new(Vec::new())),
                },
                async |_| Ok(()),
            )
            .await
            .unwrap();

        assert!(
            count.0.load(Ordering::Relaxed) > 0,
            "try_take replaced the local reply waker"
        );
        assert_eq!(reply.try_take().unwrap().unwrap_report(), 1);
        assert!(reply.try_take().is_none());
        completion.wait().await.unwrap();
    }

    let values = Rc::new(RefCell::new(Vec::new()));
    let cleaned = Rc::new(Cell::new(false));
    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();

    spawner.spawn(run_owner(owner, values.clone(), cleaned.clone(), None).unwrap());
    sensor.ready().await.unwrap();

    let mut consumed = record::request(&sensor, 0);

    assert!((&mut consumed).await.is_err());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| consumed.try_send())).is_err()
    );

    let weak = sensor.downgrade();

    drop(weak.upgrade().unwrap());

    let handle = sensor.new_handle().new_handle();
    let started = Rc::new(Signal::new());
    let release = Rc::new(Signal::new());

    let first = hold::request(&handle, started.clone(), release.clone())
        .send()
        .await;

    started.wait().await;

    drop(record::request(&sensor, 2).send().await);

    let second = record::request(&sensor, 3).send().await;
    let full = record::request(&sensor, 4).try_send();
    let Err(TrySendError::Full(request)) = full else {
        panic!("abandoning a reply released queue capacity")
    };

    assert!(request.send().now_or_never().is_none());

    let completion = sensor.shutdown();

    assert!(weak.upgrade().is_none());
    assert!(completion.wait().now_or_never().is_none());

    let observer = completion.new_observer();

    assert!(matches!(
        record(&sensor, 5).try_send(),
        Err(TrySendError::Closed(_))
    ));

    release.signal(());
    first.await.unwrap_report();
    second.await.unwrap_report();
    (&completion).await.unwrap();
    observer.await.unwrap();
    completion.await.unwrap();

    assert!(cleaned.get());
    assert_eq!(*values.borrow(), vec![1, 2, 3]);

    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();
    let answer = record::request(&sensor, 1).send().await;
    let completion = sensor.completion();

    owner
        .run_with(
            async || {
                Err(AktorSetupError {
                    diagnostics: "setup".into(),
                    data: "setup",
                })
            },
            async |_| Ok(()),
        )
        .await
        .unwrap_err();

    assert!(
        std::panic::AssertUnwindSafe(answer)
            .catch_unwind()
            .await
            .is_err()
    );

    let first_error = completion.wait().await.unwrap_err();

    assert!(matches!(
        &*first_error,
        embassy::OwnerError::Setup(AktorSetupError { data: "setup", .. })
    ));
    assert!(Rc::ptr_eq(&first_error, &sensor.ready().await.unwrap_err()));

    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();
    let reply = record(&sensor, 4).send().await;
    let (input, mut output) = record(&sensor, 5).latest();
    let completion = sensor.shutdown();

    owner
        .run(
            Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            },
            async |_| {
                Err(AktorCleanupError {
                    diagnostics: "cleanup".into(),
                    data: "cleanup",
                })
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        &*completion.wait().await.unwrap_err(),
        embassy::OwnerError::Cleanup(AktorCleanupError {
            data: "cleanup",
            ..
        })
    ));
    assert_eq!(reply.await.unwrap_report(), 1);
    assert_eq!(output.next().await.unwrap().unwrap_report(), 2);
    assert!(support::panics(output.next()).await);
    drop(input);

    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();
    let readings = Rc::new(RefCell::new(Vec::new()));
    let cleaned = Rc::new(Cell::new(false));
    let cancel = Rc::new(Signal::new());

    spawner.spawn(run_owner(owner, readings, cleaned.clone(), Some(cancel.clone())).unwrap());

    let started = Rc::new(Signal::new());
    let release = Rc::new(Signal::new());
    let running = hold::request(&sensor, started.clone(), release)
        .send()
        .await;

    started.wait().await;

    let queued = record::request(&sensor, 2).send().await;

    cancel.signal(());

    assert!(
        std::panic::AssertUnwindSafe(running)
            .catch_unwind()
            .await
            .is_err()
    );
    assert!(
        std::panic::AssertUnwindSafe(queued)
            .catch_unwind()
            .await
            .is_err()
    );
    assert!(matches!(
        &*sensor.completion().wait().await.unwrap_err(),
        embassy::OwnerError::Cancelled
    ));
    assert!(!cleaned.get());

    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();

    spawner.spawn(sensor_owner(owner).unwrap());

    let error = record(&sensor, 0).await.err().unwrap();

    assert_eq!(error.top.value, 0);
    assert_eq!(record_many(&sensor, [4, 5]).await.unwrap_report(), 2);

    let reply = record::request(&sensor, 9).send().await;
    let completion = sensor.completion();

    drop(sensor);

    assert_eq!(reply.await.unwrap_report(), 3);
    completion.wait().await.unwrap();

    listening(spawner).await;
    grouped().await;
    DONE.store(true, Ordering::Release);
}

#[test]
fn embassy() {
    if std::env::var_os("AKTOR_CHILD").is_none() {
        let output = support::child("tests::embassy", "owner");

        assert!(
            output.status.success(),
            "{}",
            std::string::String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let executor = Box::leak(Box::new(Executor::new()));

    executor.run_until(
        |spawner| spawner.spawn(exercise(spawner).unwrap()),
        || DONE.load(Ordering::Acquire),
    );
}

async fn grouped() {
    let readings = Rc::new(RefCell::new(Vec::new()));
    let values = readings.clone();
    let cleaned = Rc::new(Cell::new(false));
    let cleanup = cleaned.clone();
    let output = embassy::AktorGroup::new()
        .run(
            async move |app| {
                let sensor = app
                    .spawn::<Sensor, 2, ()>(embassy::ActorArgs {
                        name: "sensor".into(),
                        capacity: 2,
                        setup: async move || Ok(Sensor { readings: values }),
                        cleanup: async move |_| {
                            cleanup.set(true);
                            Ok(())
                        },
                    })
                    .unwrap();

                let (search, mut found) = local_search(&sensor, Rc::<str>::from("kat")).latest();

                search.send(Rc::<str>::from("katten"));
                assert_eq!(found.next().await.as_deref(), Some("katten"));
                drop(search);
                drop(found);

                for _ in 0..1000 {
                    let (sender, results) = record::latest(&sensor);
                    drop(sender);
                    drop(results);
                }

                let (sender, mut results) = record::latest(&sensor);

                sender.send(7);
                sender.send(8);
                drop(sender);
                assert_eq!(results.next().await.unwrap().unwrap_report(), 1);
                assert!(results.next().await.is_none());
                assert_eq!(record(&sensor, 9).await.unwrap_report(), 2);
                Ok::<_, AktorError>(17)
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .unwrap();

    assert_eq!(output, Some(17));
    assert_eq!(*readings.borrow(), vec![8, 9]);
    assert!(cleaned.get());

    let deferred = Rc::new(RefCell::new(None));
    let saved = deferred.clone();
    let report = embassy::AktorGroup::new()
        .run(
            async |app| {
                let sensor = app
                    .spawn::<Sensor, 2, ()>(embassy::ActorArgs {
                        name: "failed sensor".into(),
                        capacity: 2,
                        setup: async || Err(AktorSetupError::new("could not open sensor")),
                        cleanup: async |_| Ok(()),
                    })
                    .unwrap();

                let reply = record(&sensor, 1).send().await;

                *saved.borrow_mut() = Some(reply);
                core::future::pending::<()>().await;
                panic!("application continued after owner failure");
                #[allow(unreachable_code)]
                Ok::<_, AktorError>(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .unwrap_err();

    let mut reply = deferred.borrow_mut().take().unwrap();

    assert!(reply.try_take().is_none());
    assert!(reply.try_take().is_none());

    for _ in 0..2 {
        assert!(
            Pin::new(&mut reply)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }

    assert!(
        report
            .failure
            .unwrap()
            .message
            .contains("could not open sensor")
    );
}

#[embassy_executor::task(pool_size = 2)]
async fn group_owner(
    closing: LocalFuture<'static, ShutdownReport>,
    finished: Rc<Signal<NoopRawMutex, ShutdownReport>>,
) {
    finished.signal(closing.await);
}

async fn listening(spawner: Spawner) {
    for dropped in [false, true] {
        let mut actors = embassy::AktorGroup::new();

        assert!(matches!(
            actors.spawn_value::<u32, 2>("early", 0),
            Err(aktor::message::ActorError::NotStarted)
        ));

        let kill = actors.killswitch();
        let closing = actors.listen().unwrap();
        let completed = actors.completion();

        assert!(
            actors
                .listen_with(async |_| Ok::<_, AktorError>(()))
                .is_err()
        );

        let readings = Rc::new(RefCell::new(Vec::new()));
        let values = readings.clone();
        let cleaned = Rc::new(Cell::new(false));
        let cleanup = cleaned.clone();
        let setup = async move || Ok::<_, AktorError>(Sensor { readings: values });
        let cleanup = async move |_| {
            cleanup.set(true);
            Ok::<_, AktorError>(())
        };
        let mut args = ActorArgs::new("sensor", setup, cleanup);

        args.capacity = 2;

        let sensor = actors.spawn::<Sensor, 2, ()>(args).unwrap();
        let plain_readings = Rc::new(RefCell::new(Vec::new()));
        let plain = actors
            .spawn_value::<Sensor, 2>(
                "plain sensor",
                Sensor {
                    readings: plain_readings.clone(),
                },
            )
            .unwrap();

        let work = async {
            assert_eq!(record(&plain, 9).await.unwrap_report(), 1);
            assert_eq!(record(&sensor, 7).await.unwrap_report(), 1);

            if dropped {
                drop(actors);
            } else {
                drop(actors.shutdown());
                assert!(matches!(
                    actors.spawn_value::<u32, 2>("late", 0),
                    Err(aktor::message::ActorError::Closed)
                ));
            }
        };

        let finished = Rc::new(Signal::new());

        spawner.spawn(group_owner(closing, finished.clone()).unwrap());
        work.await;

        let report = finished.wait().await;

        assert!(!(&completed).await.failed());
        assert_eq!(completed.await.actors.len(), report.actors.len());
        assert!(!report.failed());
        assert!(cleaned.get());
        assert_eq!(*readings.borrow(), [7]);
        assert_eq!(*plain_readings.borrow(), [9]);
        assert_eq!(Rc::strong_count(&plain_readings), 1);
        assert!(kill.is_stopping());
    }
}

std::thread_local! {
    static LATEST: RefCell<Option<embassy::LatestSender<(u32,)>>> = const { RefCell::new(None) };
    static QUEUED: RefCell<Option<embassy::Handle<Sensor, 2, ()>>> = const { RefCell::new(None) };
}

struct QueueWake(AtomicBool);
impl std::task::Wake for QueueWake {
    fn wake(self: std::sync::Arc<Self>) {
        if !self.0.swap(true, Ordering::SeqCst) {
            QUEUED.with(|handle| {
                drop(
                    record::request(handle.borrow().as_ref().unwrap(), 2)
                        .try_send()
                        .unwrap(),
                );
            });
        }
    }
}

#[test]
fn queue_callbacks() {
    let readings = Rc::new(RefCell::new(Vec::new()));
    let (handle, owner) = embassy::channel::<Sensor, 2, ()>().unwrap();

    QUEUED.with(|stored| *stored.borrow_mut() = Some(handle.new_handle()));

    let waker = Waker::from(std::sync::Arc::new(QueueWake(AtomicBool::new(false))));
    let mut owner = Box::pin(owner.run(
        Sensor {
            readings: readings.clone(),
        },
        async |_| Ok(()),
    ));

    assert!(
        owner
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let mut first = record::request(&handle, 1).try_send().unwrap();

    handle.shutdown();

    let mut finished = false;

    for _ in 0..8 {
        if let Poll::Ready(result) = owner.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            result.unwrap();
            finished = true;
            break;
        }
    }

    assert!(finished, "queued calls did not finish");
    assert_eq!(*readings.borrow(), [1, 2]);
    assert_eq!(first.try_take().unwrap().unwrap_report(), 1);
    QUEUED.with(|stored| drop(stored.borrow_mut().take()));
}

#[test]
#[allow(unsafe_code)]
fn reply_callbacks() {
    use core::task::{RawWaker, RawWakerVTable};

    type Run = LocalFuture<'static, Result<(), Rc<embassy::OwnerError<()>>>>;
    std::thread_local! {
        static OWNER: RefCell<Option<Run>> = const { RefCell::new(None) };
    }

    fn clone(_: *const ()) -> RawWaker {
        OWNER.with(|owner| {
            if let Some(owner) = owner.borrow_mut().as_mut() {
                let _ = owner.as_mut().poll(&mut Context::from_waker(Waker::noop()));
            }
        });

        RawWaker::new(core::ptr::null(), &CALLBACKS)
    }

    fn noop(_: *const ()) {}

    const CALLBACKS: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);

    let readings = Rc::new(RefCell::new(Vec::new()));
    let (handle, owner) = embassy::channel::<Sensor, 2, ()>().unwrap();
    let mut reply = record(&handle, 1).try_send().unwrap();

    OWNER.with(|stored| {
        *stored.borrow_mut() = Some(Box::pin(owner.run(Sensor { readings }, async |_| Ok(()))));
    });

    // No pointer data or ownership, cloning drives this thread's owner once.
    let waker = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &CALLBACKS)) };
    let result = Pin::new(&mut reply).poll(&mut Context::from_waker(&waker));

    OWNER.with(|stored| drop(stored.borrow_mut().take()));

    let Poll::Ready(result) = result else {
        panic!("completion during waker clone was lost")
    };

    assert_eq!(result.unwrap_report(), 1);
}

struct LatestWake(AtomicBool);
impl std::task::Wake for LatestWake {
    fn wake(self: std::sync::Arc<Self>) {
        if !self.0.swap(true, Ordering::SeqCst) {
            LATEST.with(|sender| {
                aktor::latest::SendLatest::send(sender.borrow().as_ref().unwrap(), (2,))
            });
        }
    }
}
impl Drop for LatestWake {
    fn drop(&mut self) {
        let sender = LATEST.with(|sender| sender.borrow_mut().take());
        drop(sender);
    }
}

async fn latest_callbacks() {
    let readings = Rc::new(RefCell::new(Vec::new()));
    let (handle, owner) = embassy::channel::<Sensor, 2, ()>().unwrap();
    let (sender, mut results) = record::latest(&handle);

    LATEST.with(|stored| *stored.borrow_mut() = Some(sender.inner.clone()));

    let wake = std::sync::Arc::new(LatestWake(AtomicBool::new(false)));
    let waker = Waker::from(wake.clone());
    let mut owner = Box::pin(owner.run(
        Sensor {
            readings: readings.clone(),
        },
        async |_| Ok(()),
    ));

    assert!(
        owner
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    sender.send(1);
    assert!(wake.0.load(Ordering::SeqCst));
    assert!(
        owner
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(results.next().await.unwrap().unwrap_report(), 1);
    assert_eq!(*readings.borrow(), vec![2]);

    let mut next = Box::pin(results.next());

    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(wake);
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(next);
    drop(results);
    drop(sender);
    drop(handle);
    owner.await.unwrap();
}

use support::CountWake;

std::thread_local! {
    static CANCEL_OWNER: RefCell<Option<embassy::Owner<u32, 1, ()>>> = const { RefCell::new(None) };
}

struct CancelWake;
#[allow(clippy::manual_noop_waker, reason = "its destructor cancels the owner")]
impl std::task::Wake for CancelWake {
    fn wake(self: std::sync::Arc<Self>) {}
}
impl Drop for CancelWake {
    fn drop(&mut self) {
        let owner = CANCEL_OWNER.with(|owner| owner.borrow_mut().take());
        drop(owner);
    }
}

#[test]
fn completion_callbacks() {
    for ready in [false, true] {
        let (handle, owner) = embassy::channel::<u32, 1, ()>().unwrap();

        CANCEL_OWNER.with(|stored| *stored.borrow_mut() = Some(owner));

        let completion = handle.completion();
        let mut waiting: LocalFuture<'_, Result<(), Rc<embassy::OwnerError<()>>>> = if ready {
            Box::pin(handle.ready())
        } else {
            Box::pin(completion.wait())
        };

        let waker = Waker::from(std::sync::Arc::new(CancelWake));

        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);

        let Poll::Ready(Err(error)) = waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("owner cancellation did not reach its observer")
        };

        assert!(matches!(&*error, embassy::OwnerError::Cancelled));
    }

    struct Input;
    impl Drop for Input {
        fn drop(&mut self) {
            panic!("queued input drop failed");
        }
    }

    let (handle, owner) = embassy::channel::<u32, 1, ()>().unwrap();
    let reply = embassy::Request::new(
        &handle,
        aktor::operation::Operation {
            name: "discarded input",
            caller: core::panic::Location::caller(),
        },
        async |_: &mut u32, input: Input| drop(input),
        Input,
    )
    .send()
    .now_or_never()
    .unwrap();

    drop(reply);

    let completion = handle.completion();
    let wake = std::sync::Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut waiting = Box::pin(completion.wait());

    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).is_err());
    assert!(wake.0.load(core::sync::atomic::Ordering::Relaxed) > 0);
    assert!(
        waiting
            .now_or_never()
            .expect("input destructor stranded completion")
            .is_err()
    );
}

#[test]
fn latest_cancellation_wakes() {
    for running in [false, true] {
        let (handle, owner) = embassy::channel::<Sensor, 2, ()>().unwrap();
        let started = Rc::new(Signal::new());
        let release = Rc::new(Signal::new());
        let (sender, mut results) = hold(&handle, started, release).latest();
        let mut owner = Box::pin(owner.run(
            Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            },
            async |_| Ok(()),
        ));

        if running {
            assert!(
                owner
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }

        let wake = std::sync::Arc::new(CountWake(core::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        let mut next = Box::pin(results.next());

        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        handle.shutdown();
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        let before = wake.0.load(Ordering::SeqCst);

        drop(owner);
        assert!(
            wake.0.load(Ordering::SeqCst) > before,
            "owner cancellation lost its wake"
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                next.as_mut().poll(&mut Context::from_waker(&waker))
            }))
            .is_err()
        );
        drop(next);
        drop(sender);
    }
}

async fn driver_cancellation() {
    let waker = core::future::poll_fn(|cx| core::task::Poll::Ready(cx.waker().clone())).await;

    for shutdown in [false, true] {
        let mut group = embassy::AktorGroup::new();

        if shutdown {
            drop(group.shutdown());
        } else {
            group.killswitch().stop();
        }

        assert!(!group.shutdown().await.failed());
        assert!(group.listen().is_err());
        assert!(matches!(
            group.spawn_value::<u32, 1>("late", 7),
            Err(aktor::message::ActorError::Closed)
        ));
    }

    for stage in 0..5 {
        let mut group = embassy::AktorGroup::new();
        let completion = group.completion();
        let hook = Rc::new(Cell::new(false));
        let entered = hook.clone();
        let mut driver = group
            .listen_with(async move |_| {
                entered.set(true);

                if stage == 4 {
                    core::future::pending::<()>().await;
                }

                Ok::<_, AktorError>(())
            })
            .unwrap();

        let handle = group
            .spawn::<Sensor, 2, ()>(ActorArgs {
                name: "sensor".into(),
                capacity: 2,
                setup: async move || {
                    if stage == 1 {
                        core::future::pending::<()>().await;
                    }

                    Ok(Sensor {
                        readings: Rc::new(RefCell::new(Vec::new())),
                    })
                },
                cleanup: async move |_| {
                    if stage == 3 {
                        core::future::pending::<()>().await;
                    }

                    Ok(())
                },
            })
            .unwrap();

        if stage != 0 {
            assert!(
                driver
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }

        if stage == 2 {
            let _reply = hold(&handle, Rc::new(Signal::new()), Rc::new(Signal::new()))
                .try_send()
                .unwrap();

            assert!(
                driver
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }

        if stage >= 3 {
            drop(group.shutdown());
            assert!(
                driver
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }

        let count = std::sync::Arc::new(CountWake(core::sync::atomic::AtomicUsize::new(0)));
        let observer_waker = Waker::from(count.clone());
        let mut observed = Box::pin(completion.wait());

        assert!(
            observed
                .as_mut()
                .poll(&mut Context::from_waker(&observer_waker))
                .is_pending()
        );
        drop(group.completion());
        drop(driver);
        assert!(count.0.load(Ordering::SeqCst) > 0);

        let report = observed.now_or_never().expect("driver stranded completion");

        assert!(report.failed());
        assert!(group.listen().is_err());
        assert!(matches!(
            group.spawn_value::<u32, 1>("late", 7),
            Err(aktor::message::ActorError::Closed)
        ));
        assert_eq!(hook.get(), stage == 4);
    }

    let group = embassy::AktorGroup::new();
    let completion = group.completion();
    let mut driver = Box::pin(group.run(
        async |app| -> Result<(), AktorError> {
            let _handle = app.spawn_value::<u32, 1>("counter", 0).unwrap();
            core::future::pending().await
        },
        async |_| Ok::<_, AktorError>(()),
    ));

    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(driver);
    assert!(
        completion
            .wait()
            .now_or_never()
            .expect("run stranded completion")
            .failed()
    );
}

#[test]
fn registration_from_actor() {
    let group = Rc::new(RefCell::new(embassy::AktorGroup::new()));
    let mut driver = group.borrow_mut().listen().unwrap();
    let weak = Rc::downgrade(&group);
    let handle = group
        .borrow_mut()
        .spawn::<u32, 1, ()>(ActorArgs {
            name: "first".into(),
            capacity: 1,
            setup: async move || {
                let _second = weak
                    .upgrade()
                    .unwrap()
                    .borrow_mut()
                    .spawn_value::<u32, 1>("second", 7)
                    .unwrap();

                Ok(0)
            },
            cleanup: async |_| Ok(()),
        })
        .unwrap();

    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    let weak = Rc::downgrade(&group);
    let reply = embassy::Request::new(
        &handle,
        aktor::operation::Operation {
            name: "register",
            caller: core::panic::Location::caller(),
        },
        async move |_, ()| {
            let _third = weak
                .upgrade()
                .unwrap()
                .borrow_mut()
                .spawn_value::<u32, 1>("third", 17)
                .unwrap();

            17
        },
        (),
    )
    .try_send()
    .unwrap();

    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(reply.now_or_never(), Some(17));

    drop(handle);
    drop(group.borrow().shutdown());

    let report = driver.now_or_never().unwrap();

    assert!(!report.failed());
    assert_eq!(report.actors.len(), 3);
}

#[embassy_executor::task(pool_size = 2)]
async fn setup_driver(driver: LocalFuture<'static, ()>) {
    driver.await;
}

async fn setups(spawner: Spawner) {
    let events = Rc::new(Cell::new(0));
    let before = events.clone();
    let after = events.clone();
    let ended = Rc::new(Cell::new(false));
    let cleaned = ended.clone();
    let tick: Gate = Rc::new(Signal::new());
    let interval = tick.clone();
    let first = AktorSetup {
        name: AktorName::new("local readings"),
        role: AktorNoRole,
        kind: AktorKind::EmbassyLocal(move |future| {
            let task =
                setup_driver(future).map_err(|_| AktorSetupError::new("no actor task slot"))?;

            spawner.spawn(task);
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
                    cleaned.set(true);
                    Ok(())
                })
                .into(),
            ),
            intervals: vec![AktorInterval {
                every: core::time::Duration::from_millis(1),
                run: (async move |_: &mut Sensor| {
                    interval.signal(());
                })
                .into(),
            }],
            before_each: Some(
                (move |_: &mut Sensor, _: aktor::operation::Operation| {
                    before.set(before.get() + 1);
                })
                .into(),
            ),
            after_each: Some(
                (move |_: &mut Sensor, _: aktor::operation::Operation| {
                    after.set(after.get() + 1);
                })
                .into(),
            ),
        },
        options: None,
    };

    let second = AktorSetup {
        name: AktorName::new("another sensor"),
        role: AktorNoRole,
        kind: AktorKind::EmbassyLocal(|_| {
            Err(AktorSetupError::new("second driver must not start"))
        }),
        closures: AktorClosures {
            start: async || {
                Ok(Sensor {
                    readings: Rc::new(RefCell::new(Vec::new())),
                })
            },
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    };

    let actors = start(aktor_setups! { first, second }).await.unwrap();

    assert_eq!(record(&actors.handles.first, 85).await.unwrap_report(), 1);
    assert!(record(&actors.handles.first, 0).await.is_err());

    let (input, mut output) = record::latest(&actors.handles.first);

    input.send(1);
    input.send(2);
    assert_eq!(output.next().await.unwrap().unwrap_report(), 2);
    assert_eq!(
        record_many(&actors.handles.second, [1, 2])
            .await
            .unwrap_report(),
        2
    );
    tick.wait().await;
    assert_eq!(events.get() % 2, 0);

    let completion = actors.completion();
    let report = actors.shutdown().await;

    assert!(!report.failed(), "{report}");
    assert!(ended.get());
    assert_eq!(completion.await.actors.len(), 2);
    assert!(output.next().await.is_none());
    assert!(
        report
            .actors
            .iter()
            .all(|actor| actor.kind == Some(AktorExecution::EmbassyLocal))
    );

    let never = || AktorSetup {
        name: AktorName::new("unpolled"),
        role: AktorNoRole,
        kind: AktorKind::EmbassyLocal(|_| Err(AktorSetupError::new("must not spawn"))),
        closures: AktorClosures {
            start: async || Ok(85_u32),
            end: None,
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: None,
    };

    let startup = start(never());
    let completion = startup.completion();

    drop(startup);
    assert!(completion.await.actors.is_empty());

    let startup = start(never());
    let completion = startup.completion();

    assert!(startup.await.is_err());
    assert!(completion.await.failed());

    let actors = start(sensor_setup(spawner)).await.unwrap();

    assert_eq!(record(&actors.handles, 85).await.unwrap_report(), 1);
    assert!(!actors.shutdown().await.failed());
}
