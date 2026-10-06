use aktor::*;
use std::{
    cell::{Cell, RefCell},
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

#[aktor]
async fn add(counter: &mut Counter, by: u32) -> Rc<Cell<u32>> {
    counter.0.set(counter.0.get() + by);
    counter.0.clone()
}

pub fn check() -> Result<(), String> {
    let pool = bevy_tasks::TaskPool::new();
    let local: Result<(), String> = pool.with_local_executor(|executor| {
        aktor::executor::block_on(executor.run(async {
            let ended = Rc::new(Cell::new(false));
            let end = ended.clone();
            let local_setup = AktorSetup {
                name: AktorName::new("Bevy counter"),
                role: AktorNoRole,
                kind: AktorKind::BevyLocal(&pool),
                closures: AktorClosures {
                    start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                    end: Some(
                        (async move |counter: Counter| {
                            assert_eq!(counter.0.get(), 7);
                            end.set(true);
                            Ok(())
                        })
                        .into(),
                    ),
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: None,
            };

            let task_setup = AktorSetup {
                name: AktorName::new("Bevy task alongside local"),
                role: AktorNoRole,
                kind: AktorKind::BevyTask(&pool),
                closures: AktorClosures {
                    start: async || Ok(Movable(Cell::new(0))),
                    end: None,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: None,
            };

            let actors = start(aktor_setups! { local: local_setup, task: task_setup })
                .await
                .map_err(|error| error.to_string())?;

            assert_eq!(add(&actors.handles.local, 3).await.get(), 3);

            let (input, mut output) = add::latest(&actors.handles.local);

            input.send(4);
            assert_eq!(increment(&actors.handles.task, 5).await, 5);

            let report = actors.shutdown().await;

            assert!(!report.failed(), "{report}");
            assert!(
                report
                    .actors
                    .iter()
                    .any(|actor| actor.kind == Some(AktorExecution::BevyLocal))
            );
            assert!(
                report
                    .actors
                    .iter()
                    .any(|actor| actor.kind == Some(AktorExecution::BevyTask))
            );
            assert!(ended.get());
            assert_eq!(output.next().await.unwrap().get(), 7);
            assert!(output.next().await.is_none());
            drop(input);

            let signal = Rc::new(Signal::default());
            let notify = signal.clone();
            let actors = start(AktorSetup {
                name: AktorName::new("Bevy interval"),
                role: AktorNoRole,
                kind: AktorKind::BevyLocal(&pool),
                closures: AktorClosures {
                    start: async || Ok(Counter(Rc::new(Cell::new(0)))),
                    end: None,
                    intervals: vec![AktorInterval {
                        every: Duration::from_millis(1),
                        run: (async move |_: &mut Counter| {
                            notify.ready.set(true);
                            if let Some(wake) = notify.wake.take() {
                                wake.wake();
                            }
                            core::future::pending::<()>().await;
                        })
                        .into(),
                    }],
                    before_each: None,
                    after_each: None,
                },
                options: Some(AktorOptions {
                    shutdown_grace: Duration::from_millis(200),
                    ..Default::default()
                }),
            })
            .await
            .map_err(|error| error.to_string())?;

            std::future::poll_fn(|cx| {
                if signal.ready.get() {
                    Poll::Ready(())
                } else {
                    signal.wake.replace(Some(cx.waker().clone()));
                    Poll::Pending
                }
            })
            .await;

            let report = actors.shutdown().await;

            assert!(report.timed_out, "{report}");
            Ok(())
        }))
    });

    local?;
    aktor::executor::block_on(task(&pool))?;
    cooperative()
}

struct Busy {
    handle: Option<AktorTask<Busy>>,
    count: Arc<AtomicUsize>,
    heartbeat: Option<tokio::sync::oneshot::Sender<()>>,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

#[aktor]
async fn tick(_: &mut Busy) {}

#[aktor]
async fn begin(state: &mut Busy, handle: AktorTask<Busy>) {
    state.handle = Some(handle);
    drop(tick(state.handle.as_ref().unwrap()).try_send().unwrap());
}

fn cooperative() -> Result<(), String> {
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
        let actors = start(AktorSetup {
            name: AktorName::new("Bevy busy owner"),
            role: AktorNoRole,
            kind: AktorKind::BevyTask(&pool),
            closures: AktorClosures {
                before_each: Some(
                    (|state: &mut Busy, _: operation::Operation| {
                        let Some(handle) = &state.handle else {
                            return;
                        };
                        let count = state.count.fetch_add(1, Ordering::Relaxed) + 1;

                        if count == 1 {
                            state.heartbeat.take().unwrap().send(()).unwrap();
                        }

                        if count < 256 {
                            drop(tick(handle).try_send().unwrap());
                        } else {
                            state.done.take().unwrap().send(()).unwrap();
                        }
                    })
                    .into(),
                ),
                ..AktorClosures::new(async move || {
                    Ok(Busy {
                        handle: None,
                        count: counted,
                        heartbeat: Some(heartbeat),
                        done: Some(done),
                    })
                })
            },
            options: Some(AktorOptions {
                capacity: 1,
                ..Default::default()
            }),
        })
        .await
        .map_err(|error| error.to_string())?;

        begin(&actors.handles, actors.handles.clone()).await;
        finished.await.unwrap();
        let progress = sibling.await;
        let report = actors.shutdown().await;

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

#[aktor]
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

async fn task(pool: &bevy_tasks::TaskPool) -> Result<(), String> {
    let ended = Arc::new(Mutex::new(None));
    let end = ended.clone();
    let actors = start(AktorSetup {
        name: AktorName::new("Bevy movable counter"),
        role: AktorNoRole,
        kind: AktorKind::BevyTask(pool),
        closures: AktorClosures {
            start: async || Ok(Movable(Cell::new(0))),
            end: Some(
                (async move |counter: Movable| {
                    *end.lock().unwrap() = Some(counter.0.get());
                    Ok(())
                })
                .into(),
            ),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: Some(AktorOptions {
            shutdown_grace: Duration::from_millis(300),
            ..Default::default()
        }),
    })
    .await
    .map_err(|error| error.to_string())?;

    assert_eq!(increment(&actors.handles, 2).await, 2);

    let (notify, started) = std::sync::mpsc::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let mut reply = delayed(&actors.handles, notify, gate).send().await;

    started.recv_timeout(Duration::from_secs(1)).unwrap();

    let error = reply.timeout(Duration::from_millis(1)).await.unwrap_err();

    assert!(error.admitted);
    drop(reply);
    release.send(()).unwrap();

    let (input, mut output) = increment::latest(&actors.handles);

    input.send(4);

    let report = actors.shutdown().await;

    assert!(!report.failed(), "{report}");
    assert_eq!(report.actors[0].kind, Some(AktorExecution::BevyTask));
    assert_eq!(*ended.lock().unwrap(), Some(7));
    assert_eq!(output.next().await, Some(7));
    assert!(output.next().await.is_none());
    drop(input);

    let (notify, started) = std::sync::mpsc::channel();
    let actors = start(AktorSetup {
        name: AktorName::new("Bevy movable interval"),
        role: AktorNoRole,
        kind: AktorKind::BevyTask(pool),
        closures: AktorClosures {
            start: async || Ok(Movable(Cell::new(0))),
            end: None,
            intervals: vec![AktorInterval {
                every: Duration::from_millis(1),
                run: (move |_counter: AktorTaskState<Movable>| {
                    let _sent = notify.send(());
                    async move { core::future::pending::<()>().await }
                })
                .into(),
            }],
            before_each: None,
            after_each: None,
        },
        options: Some(AktorOptions {
            shutdown_grace: Duration::from_millis(200),
            ..Default::default()
        }),
    })
    .await
    .map_err(|error| error.to_string())?;

    started.recv_timeout(Duration::from_secs(1)).unwrap();

    let report = actors.shutdown().await;

    assert!(report.timed_out, "{report}");
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn counter() {
        super::check().unwrap();
    }
}

fn main() -> Result<(), String> {
    check()
}
