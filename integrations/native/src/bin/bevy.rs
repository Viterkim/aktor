#[path = "../../../cli.rs"]
mod cli;

use aktor::*;
use cli::Report;
use core::num::NonZeroUsize;
use std::{
    cell::{Cell, RefCell},
    process::ExitCode,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::Duration,
};

struct Counter(Rc<Cell<u32>>);

#[derive(Default)]
struct Signal {
    ready: Cell<bool>,
    wake: RefCell<Option<Waker>>,
}

#[aktor(latest)]
async fn add(counter: &mut Counter, by: u32) -> Rc<Cell<u32>> {
    counter.0.set(counter.0.get() + by);
    counter.0.clone()
}

fn check() -> Result<(), Report> {
    let pool = bevy_tasks::TaskPool::new();
    let local: Result<(), Report> = pool.with_local_executor(|executor| {
        aktor::executor::block_on(executor.run(async {
            let ended = Rc::new(Cell::new(false));
            let end = ended.clone();
            let local_setup = AktorNew {
                name: "Bevy counter".into(),
                kind: AktorKind::BevyLocal(&pool),
                start: AktorInit::Ready(async || Counter(Rc::new(Cell::new(0)))),
                end: async move |counter: Counter| {
                    assert_eq!(counter.0.get(), 7);
                    end.set(true);
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            };

            let task_setup = AktorNew {
                name: "Bevy task alongside local".into(),
                kind: AktorKind::BevyTask(&pool),
                start: AktorInit::Ready(async || Movable(Cell::new(0))),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            };

            let actors = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: (local_setup, task_setup),
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            });
            actors.ready().await?;

            assert_eq!(add(&actors.handles().0, 3).await.unwrap().get(), 3);

            let (input, mut output) = add::latest(&actors.handles().0);

            input.send(4);
            assert_eq!(increment(&actors.handles().1, 5).await.unwrap(), 5);

            let report = actors.stop().await;

            assert!(!report.failed(), "{report}");
            assert!(ended.get());
            assert_eq!(output.next().await.unwrap().unwrap().get(), 7);
            assert!(output.next().await.is_none());
            drop(input);

            let signal = Rc::new(Signal::default());
            let notify = signal.clone();
            let actors = aktor_start(AktorSetup {
                stuck: AktorGroupStuck::KeepRunning,
                actors: AktorNew {
                    name: "Bevy interval".into(),
                    kind: AktorKind::BevyLocal(&pool),
                    start: AktorInit::Ready(async || Counter(Rc::new(Cell::new(0)))),
                    end: (),
                    hooks: AktorHooks {
                        intervals: vec![
                            AktorInterval::new(
                                Duration::from_millis(1),
                                async move |_: &mut Counter| {
                                    notify.ready.set(true);
                                    if let Some(wake) = notify.wake.take() {
                                        wake.wake();
                                    }
                                    core::future::pending::<()>().await;
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
                grace: Duration::from_millis(200),
            });
            actors.ready().await?;

            std::future::poll_fn(|cx| {
                if signal.ready.get() {
                    Poll::Ready(())
                } else {
                    signal.wake.replace(Some(cx.waker().clone()));
                    Poll::Pending
                }
            })
            .await;

            let report = actors.stop().await;

            assert!(report.ran_past_grace(), "{report}");
            Ok(())
        }))
    });

    local?;
    aktor::executor::block_on(task(&pool))?;
    cooperative()
}

struct Busy {
    count: Arc<AtomicUsize>,
    heartbeat: Option<tokio::sync::oneshot::Sender<()>>,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

#[aktor]
async fn tick(_: &mut Busy) {}

fn cooperative() -> Result<(), Report> {
    let pool = bevy_tasks::TaskPoolBuilder::new().num_threads(1).build();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let counted = count.clone();
    let (heartbeat, waiting) = tokio::sync::oneshot::channel();
    let (done, finished) = tokio::sync::oneshot::channel();
    let sibling = pool.spawn(async move {
        waiting.await.unwrap();
        observed.load(Ordering::Relaxed)
    });

    aktor::executor::block_on(async {
        let actors = aktor_start(AktorSetup {
            stuck: AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "Bevy busy owner".into(),
                kind: AktorKind::BevyTask(&pool),
                start: AktorInit::Ready(async move || Busy {
                    count: counted,
                    heartbeat: Some(heartbeat),
                    done: Some(done),
                }),
                end: (),
                hooks: AktorHooks {
                    intervals: vec![],
                    before_each: |state: &mut Busy, _: operation::Operation| {
                        let count = state.count.fetch_add(1, Ordering::Relaxed) + 1;

                        if count == 1 {
                            state.heartbeat.take().unwrap().send(()).unwrap();
                        }

                        if count == 256 {
                            state.done.take().unwrap().send(()).unwrap();
                        }
                    },
                    after_each: (),
                },
                capacity: NonZeroUsize::MIN,
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });
        actors.ready().await?;

        for _ in 0..256 {
            tick(actors.handles()).cast().await.unwrap();
        }
        finished.await.unwrap();
        let progress = sibling.await;
        let report = actors.stop().await;

        assert!(!report.failed(), "{report}");
        assert_eq!(count.load(Ordering::Relaxed), 256);
        assert!(
            progress > 0 && progress < 256,
            "heartbeat saw {progress} jobs"
        );
        Ok(())
    })
}

struct Movable(Cell<u32>);

#[aktor(latest)]
async fn increment(counter: &mut Movable, by: u32) -> u32 {
    counter.0.set(counter.0.get() + by);
    counter.0.get()
}

#[aktor]
async fn delayed(
    counter: &mut Movable,
    started: std::sync::mpsc::Sender<()>,
    gate: tokio::sync::oneshot::Receiver<()>,
) -> u32 {
    let _sent = started.send(());
    let _released = gate.await;

    counter.0.set(counter.0.get() + 1);
    counter.0.get()
}

async fn task(pool: &bevy_tasks::TaskPool) -> Result<(), Report> {
    let ended = Arc::new(Mutex::new(None));
    let end = ended.clone();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "Bevy movable counter".into(),
            kind: AktorKind::BevyTask(pool),
            start: AktorInit::Ready(async || Movable(Cell::new(0))),
            end: async move |counter: Movable| {
                *end.lock().unwrap() = Some(counter.0.get());
            },
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_millis(300),
    });
    actors.ready().await?;

    assert_eq!(increment(actors.handles(), 2).await.unwrap(), 2);

    let (notify, started) = std::sync::mpsc::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let mut reply = delayed(actors.handles(), notify, gate)
        .send()
        .await
        .unwrap();

    started.recv_timeout(Duration::from_secs(1)).unwrap();

    let error = reply.timeout(Duration::from_millis(1)).await.unwrap_err();

    assert!(error.admitted);
    drop(reply);
    release.send(()).unwrap();

    let (input, mut output) = increment::latest(actors.handles());

    input.send(4);

    let report = actors.stop().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(*ended.lock().unwrap(), Some(7));
    assert_eq!(output.next().await, Some(Ok(7)));
    assert!(output.next().await.is_none());
    drop(input);

    let (notify, started) = std::sync::mpsc::channel();
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "Bevy movable interval".into(),
            kind: AktorKind::BevyTask(pool),
            start: AktorInit::Ready(async || Movable(Cell::new(0))),
            end: (),
            hooks: AktorHooks {
                intervals: vec![
                    AktorInterval::new(
                        Duration::from_millis(1),
                        move |_counter: AktorTaskState<Movable>| {
                            let _sent = notify.send(());
                            async move { core::future::pending::<()>().await }
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
        grace: Duration::from_millis(200),
    });
    actors.ready().await?;

    started.recv_timeout(Duration::from_secs(1)).unwrap();

    let report = actors.stop().await;

    assert!(report.ran_past_grace(), "{report}");
    Ok(())
}

fn main() -> ExitCode {
    cli::exit(check())
}

#[cfg(test)]
mod tests {
    #[test]
    fn counter() {
        assert_eq!(super::main(), std::process::ExitCode::SUCCESS);
    }
}
