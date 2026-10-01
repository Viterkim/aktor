extern crate std;

use super::*;
use aktor::message::{CallError, TrySendError};
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

        let reply = request.try_send().unwrap();
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

        assert_eq!(reply.await.unwrap_report(), 1);
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

    assert!(request.checked_send().now_or_never().is_none());

    let completion = sensor.shutdown();
    assert!(weak.upgrade().is_none());
    assert!(completion.wait().now_or_never().is_none());

    let observer = completion.new_observer();
    assert!(matches!(
        record::request(&sensor, 5).checked().await,
        Err(CallError::NotAdmitted)
    ));

    release.signal(());
    first.await.unwrap_report();
    second.await.unwrap_report();
    observer.wait().await.unwrap();
    completion.wait().await.unwrap();

    assert!(cleaned.get());
    assert_eq!(*values.borrow(), vec![1, 2, 3]);

    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();
    let answer = record::request(&sensor, 1).send().await.checked();
    let completion = sensor.completion();

    owner
        .run_with(async || Err("setup"), async |_| Ok(()))
        .await
        .unwrap_err();

    assert!(matches!(answer.await, Err(CallError::Discarded)));
    let first_error = completion.wait().await.unwrap_err();
    assert!(matches!(&*first_error, embassy::OwnerError::Setup("setup")));
    assert!(Rc::ptr_eq(&first_error, &sensor.ready().await.unwrap_err()));

    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();
    let completion = sensor.shutdown();

    owner
        .run(
            Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            },
            async |_| Err("cleanup"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        &*completion.wait().await.unwrap_err(),
        embassy::OwnerError::Cleanup("cleanup")
    ));

    let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>().unwrap();
    let readings = Rc::new(RefCell::new(Vec::new()));
    let cleaned = Rc::new(Cell::new(false));
    let cancel = Rc::new(Signal::new());
    spawner.spawn(run_owner(owner, readings, cleaned.clone(), Some(cancel.clone())).unwrap());

    let started = Rc::new(Signal::new());
    let release = Rc::new(Signal::new());
    let running = hold::request(&sensor, started.clone(), release)
        .checked_send()
        .await
        .unwrap();
    started.wait().await;

    let queued = record::request(&sensor, 2).checked_send().await.unwrap();
    cancel.signal(());

    assert!(matches!(running.await, Err(CallError::OutcomeUnknown)));
    assert!(matches!(queued.await, Err(CallError::Discarded)));
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
