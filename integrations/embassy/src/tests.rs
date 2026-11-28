use core::future::IntoFuture;
extern crate std;

use super::*;
use aktor::{AktorShutdownReport, message::LocalFuture};
use alloc::{boxed::Box, string::String, vec};
use core::num::NonZeroUsize;
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
// aktor's own test helpers, some are gated on aktor features this crate doesn't have.
#[allow(unexpected_cfgs)]
#[path = "../../../aktor/tests/support/mod.rs"]
pub mod support;

static DONE: AtomicBool = AtomicBool::new(false);

type Gate = Rc<Signal<NoopRawMutex, ()>>;
type Owner = embassy::Owner<Sensor, &'static str>;

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
        let (sensor, owner) =
            embassy::channel::<Sensor, &'static str>(NonZeroUsize::new(2).unwrap());
        let mut request = record(&sensor, 1).into_future();

        assert!(
            Pin::new(&mut request)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );

        if close_first {
            drop(sensor.shutdown());
        }

        let mut reply = request.send().await.unwrap();

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
        assert_eq!(reply.try_take().unwrap().unwrap().er_unwrap(), 1);
        assert!(reply.try_take().is_none());
        completion.wait().await.unwrap();
    }

    let values = Rc::new(RefCell::new(Vec::new()));
    let cleaned = Rc::new(Cell::new(false));
    let (sensor, owner) = embassy::channel::<Sensor, &'static str>(NonZeroUsize::new(2).unwrap());

    spawner.spawn(run_owner(owner, values.clone(), cleaned.clone(), None).unwrap());
    sensor.ready().await.unwrap();

    let mut consumed = record(&sensor, 0).into_future();

    assert!((&mut consumed).await.unwrap().is_err());
    assert!(
        std::panic::AssertUnwindSafe(consumed.send())
            .catch_unwind()
            .await
            .is_err()
    );

    let weak = sensor.downgrade();

    drop(weak.upgrade().unwrap());

    let handle = sensor.clone();
    let started = Rc::new(Signal::new());
    let release = Rc::new(Signal::new());

    let first = hold(&handle, started.clone(), release.clone())
        .send()
        .await
        .unwrap();

    started.wait().await;

    drop(record(&sensor, 2).send().await.unwrap());

    let second = record(&sensor, 3).send().await.unwrap();
    assert!(record(&sensor, 4).send().now_or_never().is_none());

    let completion = sensor.shutdown();

    assert!(weak.upgrade().is_none());
    assert!(completion.wait().now_or_never().is_none());

    let observer = completion.clone();

    assert_eq!(
        record(&sensor, 5).send().await.err().unwrap().outcome,
        aktor::AktorDeliveryOutcome::NotAdmitted
    );

    release.signal(());
    first.await.unwrap().er_unwrap();
    second.await.unwrap().er_unwrap();
    (&completion).await.unwrap();
    observer.await.unwrap();
    completion.await.unwrap();

    assert!(cleaned.get());
    assert_eq!(*values.borrow(), vec![1, 2, 3]);

    let (sensor, owner) = embassy::channel::<Sensor, &'static str>(NonZeroUsize::new(2).unwrap());
    let answer = record(&sensor, 1).send().await.unwrap();
    let completion = owner.completion();

    owner
        .run_with(async || Err(("setup", "setup".into())), async |_| Ok(()))
        .await
        .unwrap_err();

    assert_eq!(
        answer.await.err().unwrap().outcome,
        aktor::AktorDeliveryOutcome::Discarded
    );

    let (first_error, _) = completion.wait_with_data().await.unwrap_err();

    assert!(matches!(
        &*first_error.cause,
        embassy::OwnerError::Setup("setup")
    ));
    assert!(sensor.ready().await.is_err());

    let (sensor, owner) = embassy::channel::<Sensor, &'static str>(NonZeroUsize::new(2).unwrap());
    let reply = record(&sensor, 4).send().await.unwrap();
    let (input, mut output) = record(&sensor, 5).latest();
    let completion = owner.completion();
    drop(sensor.shutdown());

    owner
        .run(
            Sensor {
                readings: Rc::new(RefCell::new(Vec::new())),
            },
            async |_| Err(("cleanup", "cleanup".into())),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        &*completion.wait_with_data().await.unwrap_err().0.cause,
        embassy::OwnerError::Cleanup("cleanup")
    ));
    assert_eq!(reply.await.unwrap().er_unwrap(), 1);
    assert_eq!(output.next().await.unwrap().unwrap().er_unwrap(), 2);
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
    drop(input);

    let (sensor, owner) = embassy::channel::<Sensor, &'static str>(NonZeroUsize::new(2).unwrap());
    let readings = Rc::new(RefCell::new(Vec::new()));
    let cleaned = Rc::new(Cell::new(false));
    let cancel = Rc::new(Signal::new());

    spawner.spawn(run_owner(owner, readings, cleaned.clone(), Some(cancel.clone())).unwrap());

    let started = Rc::new(Signal::new());
    let release = Rc::new(Signal::new());
    let running = hold(&sensor, started.clone(), release)
        .send()
        .await
        .unwrap();

    started.wait().await;

    let queued = record(&sensor, 2).send().await.unwrap();

    cancel.signal(());

    assert_eq!(
        running.await.err().unwrap().outcome,
        aktor::AktorDeliveryOutcome::OutcomeUnknown
    );
    assert_eq!(
        queued.await.err().unwrap().outcome,
        aktor::AktorDeliveryOutcome::Discarded
    );
    assert!(matches!(
        sensor.completion().wait().await.unwrap_err().0,
        embassy::OwnerError::Cancelled
    ));
    assert!(!cleaned.get());

    let (sensor, owner) = embassy::channel::<Sensor, &'static str>(NonZeroUsize::new(2).unwrap());

    spawner.spawn(sensor_owner(owner).unwrap());

    let error = record(&sensor, 0).await.unwrap().err().unwrap();

    assert_eq!(error.top.value, 0);
    assert_eq!(record_many(&sensor, [4, 5]).await.unwrap().er_unwrap(), 2);

    let reply = record(&sensor, 9).send().await.unwrap();
    let completion = sensor.completion();

    drop(sensor);

    assert_eq!(reply.await.unwrap().er_unwrap(), 3);
    completion.wait().await.unwrap();

    listening(spawner).await;
    grouped().await;
    DONE.store(true, Ordering::Release);
}

#[test]
fn embassy() {
    if std::env::var_os("AKTOR_TEST_CASE").is_none() {
        let output = support::run_test("tests::embassy", "owner");

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

fn member_kind() -> AktorKind::EmbassyLocal {
    AktorKind::EmbassyLocal(|_| Err(((), String::from("group driver already running"))))
}

fn value<S: 'static>(
    name: &str,
    state: S,
) -> impl embassy::AktorDefinition<embassy::Handle<S, ()>> {
    AktorNew {
        name: name.into(),
        kind: member_kind(),
        start: aktor::AktorInit::Ready(async move || state),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    }
}

async fn grouped() {
    let readings = Rc::new(RefCell::new(Vec::new()));
    let values = readings.clone();
    let cleaned = Rc::new(Cell::new(false));
    let cleanup = cleaned.clone();
    let mut group = embassy::AktorGroup::new();
    let closing = group.listen(async |_| ()).unwrap();
    let killswitch = group.killswitch();
    let work = async {
        let output = killswitch
            .run(async move {
                let sensor = group
                    .spawn(AktorNew {
                        name: "sensor".into(),
                        kind: member_kind(),
                        start: aktor::AktorInit::Ready(async move || Sensor { readings: values }),
                        end: move |_: Sensor| cleanup.set(true),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(2).unwrap(),
                        stuck: AktorStuck::Kill,
                    })
                    .ready()
                    .await
                    .unwrap();

                let (search, mut found) = local_search(&sensor, Rc::<str>::from("kat")).latest();

                search.send(Rc::<str>::from("katten"));
                assert_eq!(
                    found.next().await.transpose().unwrap().as_deref(),
                    Some("katten")
                );
                drop(search);
                drop(found);

                let (sender, mut results) = record::latest(&sensor);

                sender.send(7);
                sender.send(8);
                drop(sender);
                assert_eq!(results.next().await.unwrap().unwrap().er_unwrap(), 1);
                assert!(results.next().await.is_none());
                assert_eq!(record(&sensor, 9).await.unwrap().er_unwrap(), 2);
                17
            })
            .await;

        killswitch.stop();
        output
    };
    let (_report, output) = futures_util::future::join(closing, work).await;

    assert_eq!(output, Some(17));
    assert_eq!(*readings.borrow(), vec![8, 9]);
    assert!(cleaned.get());

    let deferred = Rc::new(RefCell::new(None));
    let saved = deferred.clone();
    let mut group = embassy::AktorGroup::new();
    let closing = group.listen(async |_| ()).unwrap();
    let killswitch = group.killswitch();
    let work = async {
        killswitch
            .run(async {
                let sensor = group
                    .spawn(AktorNew {
                        name: "failed sensor".into(),
                        kind: member_kind(),
                        start: aktor::AktorInit::Try(async || {
                            Err::<Sensor, _>(((), String::from("could not open sensor")))
                        }),
                        end: (),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(2).unwrap(),
                        stuck: AktorStuck::Kill,
                    })
                    .into_parts()
                    .unwrap()
                    .1;

                let reply = record(&sensor, 1).send().await.unwrap();

                *saved.borrow_mut() = Some(reply);
                core::future::pending::<()>().await;
            })
            .await
    };
    let (report, _) = futures_util::future::join(closing, work).await;

    let mut reply = deferred.borrow_mut().take().unwrap();

    let error = reply.try_take().unwrap().err().unwrap();
    assert_eq!(error.failure_id, Some(report.failures[0].id));
    assert!(report.failures[0].error.contains("could not open sensor"));
}

#[embassy_executor::task(pool_size = 2)]
async fn group_owner(
    closing: LocalFuture<'static, AktorShutdownReport>,
    finished: Rc<Signal<NoopRawMutex, AktorShutdownReport>>,
) {
    finished.signal(closing.await);
}

async fn listening(spawner: Spawner) {
    for dropped in [false, true] {
        let mut actors = embassy::AktorGroup::new();

        assert!(matches!(
            actors.spawn(value("early", 0_u32)).ready().await,
            Err((error, _)) if error.reason() == AktorStartReason::InvalidSetup
        ));

        let kill = actors.killswitch();
        let closing = actors.listen(async |_| ()).unwrap();
        let completed = actors.completion();

        assert!(actors.listen(async |_| ()).is_err());

        let readings = Rc::new(RefCell::new(Vec::new()));
        let values = readings.clone();
        let cleaned = Rc::new(Cell::new(false));
        let cleanup = cleaned.clone();
        let sensor = actors
            .spawn(AktorNew {
                name: "sensor".into(),
                kind: member_kind(),
                start: aktor::AktorInit::Ready(async move || Sensor { readings: values }),
                end: move |_: Sensor| cleanup.set(true),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(2).unwrap(),
                stuck: AktorStuck::Kill,
            })
            .into_parts()
            .unwrap()
            .1;

        let plain_readings = Rc::new(RefCell::new(Vec::new()));
        let plain = actors
            .spawn(value(
                "plain sensor",
                Sensor {
                    readings: plain_readings.clone(),
                },
            ))
            .into_parts()
            .unwrap()
            .1;

        let work = async {
            assert_eq!(record(&plain, 9).await.unwrap().er_unwrap(), 1);
            assert_eq!(record(&sensor, 7).await.unwrap().er_unwrap(), 1);

            if dropped {
                drop(actors);
            } else {
                drop(actors.stop());
                assert!(matches!(
                    actors.spawn(value("late", 0_u32)).ready().await,
                    Err((error, _)) if error.reason() == AktorStartReason::Stopping
                ));
            }
        };

        let finished = Rc::new(Signal::new());

        spawner.spawn(group_owner(closing, finished.clone()).unwrap());
        work.await;

        let report = finished.wait().await;

        assert!(!(&completed).await.unwrap().failed());
        assert_eq!(completed.await.unwrap().actors.len(), report.actors.len());
        assert!(!report.failed());
        assert!(cleaned.get());
        assert_eq!(*readings.borrow(), [7]);
        assert_eq!(*plain_readings.borrow(), [9]);
        assert_eq!(Rc::strong_count(&plain_readings), 1);
        assert!(kill.is_stopping());
    }
}

std::thread_local! {
    static LATEST: RefCell<Option<record::Sender<embassy::LatestSender<(u32,)>>>> = const { RefCell::new(None) };
    static QUEUED: RefCell<Option<embassy::Handle<Sensor, ()>>> = const { RefCell::new(None) };
}

struct QueueWake(AtomicBool);
impl std::task::Wake for QueueWake {
    fn wake(self: std::sync::Arc<Self>) {
        if !self.0.swap(true, Ordering::SeqCst) {
            QUEUED.with(|handle| {
                drop(
                    record(handle.borrow().as_ref().unwrap(), 2)
                        .send()
                        .now_or_never()
                        .unwrap()
                        .unwrap(),
                );
            });
        }
    }
}

#[test]
fn queue_callbacks() {
    let readings = Rc::new(RefCell::new(Vec::new()));
    let (handle, owner) = embassy::channel::<Sensor, ()>(NonZeroUsize::new(2).unwrap());

    QUEUED.with(|stored| *stored.borrow_mut() = Some(handle.clone()));

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

    let mut first = record(&handle, 1).send().now_or_never().unwrap().unwrap();

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
    assert_eq!(first.try_take().unwrap().unwrap().er_unwrap(), 1);
    QUEUED.with(|stored| drop(stored.borrow_mut().take()));
}

#[test]
#[allow(unsafe_code)]
fn reply_callbacks() {
    use core::task::{RawWaker, RawWakerVTable};

    type Run = LocalFuture<'static, Result<(), (embassy::OwnerError, String)>>;
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
    let (handle, owner) = embassy::channel::<Sensor, ()>(NonZeroUsize::new(2).unwrap());
    let mut reply = record(&handle, 1).send().now_or_never().unwrap().unwrap();

    OWNER.with(|stored| {
        *stored.borrow_mut() = Some(Box::pin(owner.run(Sensor { readings }, async |_| Ok(()))));
    });

    // The null data pointer owns nothing. Cloning polls only this thread's owner.
    let waker = unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &CALLBACKS)) };
    let result = Pin::new(&mut reply).poll(&mut Context::from_waker(&waker));

    OWNER.with(|stored| drop(stored.borrow_mut().take()));

    let Poll::Ready(result) = result else {
        panic!("completion during waker clone was lost")
    };

    assert_eq!(result.unwrap().er_unwrap(), 1);
}

struct LatestWake(AtomicBool);
impl std::task::Wake for LatestWake {
    fn wake(self: std::sync::Arc<Self>) {
        if !self.0.swap(true, Ordering::SeqCst) {
            LATEST.with(|sender| sender.borrow().as_ref().unwrap().send(2));
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
    let (handle, owner) = embassy::channel::<Sensor, ()>(NonZeroUsize::new(2).unwrap());
    let (sender, mut results) = record::latest(&handle);

    LATEST.with(|stored| *stored.borrow_mut() = Some(sender.clone()));

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
    assert_eq!(results.next().await.unwrap().unwrap().er_unwrap(), 1);
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
    static CANCEL_OWNER: RefCell<Option<embassy::Owner<u32, ()>>> = const { RefCell::new(None) };
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
        let (handle, owner) = embassy::channel::<u32, ()>(NonZeroUsize::MIN);

        CANCEL_OWNER.with(|stored| *stored.borrow_mut() = Some(owner));

        let completion = handle.completion();
        let mut waiting: LocalFuture<'_, Result<(), (embassy::OwnerError, String)>> = if ready {
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

        assert!(matches!(error.0, embassy::OwnerError::Cancelled));
    }

    struct Input;
    impl Drop for Input {
        fn drop(&mut self) {
            panic!("queued input drop failed");
        }
    }

    let (handle, owner) = embassy::channel::<u32, ()>(NonZeroUsize::MIN);
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
    .unwrap()
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
        let (handle, owner) = embassy::channel::<Sensor, ()>(NonZeroUsize::new(2).unwrap());
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
        assert!(matches!(
            next.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Some(Err(_)))
        ));
        drop(next);
        assert!(matches!(
            support::poll(Box::pin(results.next())),
            Poll::Ready(None)
        ));
        drop(sender);
    }
}

async fn driver_cancellation() {
    let waker = core::future::poll_fn(|cx| core::task::Poll::Ready(cx.waker().clone())).await;

    for shutdown in [false, true] {
        let mut group = embassy::AktorGroup::new();

        if shutdown {
            drop(group.stop());
        } else {
            group.killswitch().stop();
        }

        assert!(!group.stop().await.failed());
        assert!(group.listen(async |_| ()).is_err());
        assert!(matches!(
            group.spawn(value("late", 7_u32)).ready().await,
            Err((error, _)) if error.reason() == AktorStartReason::Stopping
        ));
    }

    for stage in 0..5 {
        let mut group = embassy::AktorGroup::new();
        let completion = group.completion();
        let hook = Rc::new(Cell::new(false));
        let entered = hook.clone();
        let mut driver = group
            .listen(async move |_| {
                entered.set(true);

                if stage == 4 {
                    core::future::pending::<()>().await;
                }
            })
            .unwrap();

        let handle = group
            .spawn(AktorNew {
                name: "sensor".into(),
                kind: member_kind(),
                start: aktor::AktorInit::Ready(async move || {
                    if stage == 1 {
                        core::future::pending::<()>().await;
                    }

                    Sensor {
                        readings: Rc::new(RefCell::new(Vec::new())),
                    }
                }),
                end: async move |_: Sensor| {
                    if stage == 3 {
                        core::future::pending::<()>().await;
                    }
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(2).unwrap(),
                stuck: AktorStuck::Kill,
            })
            .into_parts()
            .unwrap()
            .1;

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
                .send()
                .await
                .unwrap();

            assert!(
                driver
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }

        if stage >= 3 {
            drop(group.stop());
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

        let report = observed
            .now_or_never()
            .expect("driver stranded completion")
            .unwrap();

        assert!(report.failed());
        assert!(group.listen(async |_| ()).is_err());
        assert!(matches!(
            group.spawn(value("late", 7_u32)).ready().await,
            Err((error, _)) if error.reason() == AktorStartReason::Stopping
        ));
        assert_eq!(hook.get(), stage == 4);
    }
}

#[test]
fn registration_from_actor() {
    let mut group = embassy::AktorGroup::new();
    let mut driver = group.listen(async |_| ()).unwrap();
    let group = Rc::new(group);
    let weak = Rc::downgrade(&group);
    let handle = group
        .spawn(AktorNew {
            name: "first".into(),
            kind: member_kind(),
            start: aktor::AktorInit::Ready(async move || {
                let _second = weak.upgrade().unwrap().spawn(value("second", 7_u32));

                0_u32
            }),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        })
        .into_parts()
        .unwrap()
        .1;

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
            let _third = weak.upgrade().unwrap().spawn(value("third", 17_u32));

            17
        },
        (),
    )
    .send()
    .now_or_never()
    .unwrap()
    .unwrap();

    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(reply.now_or_never(), Some(Ok(17)));

    drop(handle);
    drop(group.stop());

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
    let first = AktorNew {
        name: "local readings".into(),
        kind: AktorKind::EmbassyLocal(move |future| {
            let task = setup_driver(future).er(())?;

            spawner.spawn(task);
            Ok::<(), (crate::SpawnDriverErr, String)>(())
        }),
        start: aktor::AktorInit::Ready(async || Sensor {
            readings: Rc::new(RefCell::new(Vec::new())),
        }),
        end: async move |_: Sensor| {
            cleaned.set(true);
        },
        hooks: AktorHooks {
            intervals: vec![
                AktorInterval::new(
                    core::time::Duration::from_millis(1),
                    async move |_: &mut Sensor| {
                        interval.signal(());
                    },
                )
                .unwrap(),
            ],
            before_each: move |_: &mut Sensor, _: aktor::operation::Operation| {
                before.set(before.get() + 1);
            },
            after_each: move |_: &mut Sensor, _: aktor::operation::Operation| {
                after.set(after.get() + 1);
            },
        },
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    let second = AktorNew {
        name: "another sensor".into(),
        kind: AktorKind::EmbassyLocal(|_| Err(((), String::from("second driver must not start")))),
        start: aktor::AktorInit::Ready(async || Sensor {
            readings: Rc::new(RefCell::new(Vec::new())),
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: (first, second),
        shutdown: |_| {},
        grace: core::time::Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    assert_eq!(
        record(&actors.handles().0, 85).await.unwrap().er_unwrap(),
        1
    );
    assert!(record(&actors.handles().0, 0).await.unwrap().is_err());

    let (input, mut output) = record::latest(&actors.handles().0);

    input.send(1);
    input.send(2);
    assert_eq!(output.next().await.unwrap().unwrap().er_unwrap(), 2);
    assert_eq!(
        record_many(&actors.handles().1, [1, 2])
            .await
            .unwrap()
            .er_unwrap(),
        2
    );
    tick.wait().await;
    assert_eq!(events.get() % 2, 0);

    let completion = actors.completion();
    let report = actors.stop().await;

    assert!(!report.failed(), "{report}");
    assert!(ended.get());
    assert_eq!(completion.await.unwrap().actors.len(), 2);
    assert!(output.next().await.is_none());
    assert!(
        report
            .actors
            .iter()
            .all(|actor| actor.kind == AktorExecution::EmbassyLocal)
    );

    let never = || AktorNew {
        name: "unpolled".into(),
        kind: AktorKind::EmbassyLocal(|_| Err(((), String::from("must not spawn")))),
        start: aktor::AktorInit::Ready(async || 85_u32),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: never(),
        shutdown: |_| {},
        grace: core::time::Duration::from_secs(5),
    });
    let completion = started.completion();

    assert!(started.ready().await.is_err());
    assert!(completion.await.unwrap().failed());

    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: sensor_setup(spawner),
        shutdown: |_| {},
        grace: core::time::Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    assert_eq!(record(actors.handles(), 85).await.unwrap().er_unwrap(), 1);
    assert!(!actors.stop().await.failed());
}
