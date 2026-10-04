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
    task::{Context, Waker},
};
use embassy_executor::{Executor, Spawner};
use futures_util::{FutureExt, future::select};

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
        assert!(actors.listen_with(async |_| Ok::<_, AktorError>(())).is_err());
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
