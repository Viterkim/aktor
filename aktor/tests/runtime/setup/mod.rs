#[cfg(feature = "local")]
mod drivers;
mod intervals;
#[cfg(any(feature = "local", feature = "std_thread"))]
mod lifecycle;
mod panics;
mod startup;

#[cfg(feature = "local")]
use super::support;
use aktor::*;
use futures_util::FutureExt;
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

struct Counter(Rc<Cell<u32>>);

#[test]
fn caller_runtime() {
    let Ok(case) = std::env::var("AKTOR_CHILD") else {
        for case in ["stop", "drop", "live", "task stop", "task runtime exit"] {
            let output = super::support::child("setup::caller_runtime", case);
            assert!(
                output.status.success(),
                "{case}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        return;
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if case.starts_with("task ") {
        let (entered, running) = tokio::sync::oneshot::channel();
        let (finish, finishing) = tokio::sync::oneshot::channel();
        let (cleaned, cleanup) = tokio::sync::oneshot::channel();
        let actors = runtime
            .block_on(aktor_start(AktorSetup {
                actors: AktorNew {
                    name: AktorName::new("caller task"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioTask,
                    closures: AktorClosures {
                        start: async || Ok::<_, AktorSetupError>(()),
                        end: async move |_: ()| {
                            entered.send(()).unwrap();
                            finishing.await.unwrap();
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            cleaned.send(()).unwrap();
                            Ok::<_, AktorCleanupError>(())
                        },
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: Default::default(),
                },
                shutdown: |_| {},
                options: AktorOptions {
                    shutdown_grace: Duration::from_secs(1),
                },
            }))
            .unwrap();
        let completion = actors.completion();
        actors.killswitch().stop();
        runtime.block_on(running).unwrap();

        if case == "task stop" {
            finish.send(()).unwrap();
            let report = runtime.block_on(completion.wait());

            assert!(!report.failed(), "{report}");
            runtime.block_on(cleanup).unwrap();
            drop(runtime);
        } else {
            drop(runtime);
            assert!(finish.send(()).is_err());
            assert!(executor::block_on(cleanup).is_err());

            let report = executor::block_on(completion.wait());

            assert!(report.failed(), "{report}");
            assert!(!report.timed_out, "{report}");
            assert!(
                report
                    .to_string()
                    .contains("actor task cancelled before cleanup completed")
            );
            assert_eq!(report.actors[0].kind, Some(AktorExecution::TokioTask));
        }

        drop(actors);
        return;
    }

    let (entered, running) = std::sync::mpsc::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (cleaned, cleanup) = std::sync::mpsc::channel();
    let (finish, finishing) = tokio::sync::oneshot::channel();
    let mut finishing = Some(finishing);
    let caller_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let interval_closed = caller_closed.clone();
    let (ticked, ticks) = std::sync::mpsc::channel();
    let actors = runtime
        .block_on(aktor_start(AktorSetup {
            actors: AktorNew {
                name: AktorName::new("caller runtime"),
                role: AktorNoRole,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(0usize),
                    intervals: if case == "live" {
                        vec![AktorInterval {
                            every: Duration::from_millis(1),
                            run: (async move |state: &mut usize| {
                                if interval_closed.load(std::sync::atomic::Ordering::Acquire) {
                                    let _sent = ticked.send(*state);
                                }
                            })
                            .into(),
                        }]
                    } else {
                        Vec::new()
                    },
                    end: async move |state: usize| {
                        cleaned.send(state).unwrap();
                        finishing.take().unwrap().await.unwrap();
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    },

                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
            shutdown: |_| {},
            options: AktorOptions {
                shutdown_grace: Duration::from_secs(1),
            },
        }))
        .unwrap();
    let handle = actors.handles.new_handle();
    let completion = actors.completion();
    let owner = actors.handles.completion();

    let reply = runtime.block_on(
        message::call_async(
            &handle,
            async move |state: &mut usize, ()| {
                entered.send(()).unwrap();
                released.await.unwrap();
                *state += 1;
            },
            (),
        )
        .send(),
    );
    running.recv_timeout(Duration::from_secs(1)).unwrap();
    drop(reply);

    if case == "stop" {
        let _closing = actors.shutdown();
    }

    drop(runtime);
    caller_closed.store(true, std::sync::atomic::Ordering::Release);
    release.send(()).unwrap();

    if case == "live" {
        assert_eq!(ticks.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
    }

    drop(actors);
    assert_eq!(cleanup.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
    finish.send(()).unwrap();

    let report = executor::block_on(completion.wait());
    assert!(!report.failed(), "{report}");
    assert!(executor::block_on(owner.wait()).is_ok());
    assert_eq!(report.actors.len(), 1);
    drop(handle);
}

mod aktors {
    pub struct Users;
    pub struct Archive;
}

#[aktor(role = aktors::Users)]
async fn user_count(counter: &u32) -> u32 {
    *counter
}

#[aktor(role = aktors::Archive)]
async fn archive_count<T: From<u32>>(counter: &u32) -> T {
    T::from(*counter)
}

#[cfg(feature = "local")]
#[aktor(role = aktors::Users)]
async fn local_add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    add(counter, by).await
}

#[aktor]
async fn add(counter: &mut Counter, by: u32) -> Result<u32, &'static str> {
    if by == 0 {
        return Err("give me something");
    }

    let value = counter.0.get() + by;

    counter.0.set(value);
    Ok(value)
}

#[tokio::test]
async fn counter() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let before = log.clone();
    let after = log.clone();
    let end = log.clone();
    let setup = AktorNew {
        name: AktorName::new("BingoManden"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok::<_, AktorSetupError>(Counter(Rc::new(Cell::new(0)))),
            end: async move |counter: Counter| {
                end.lock().unwrap().push(("end", counter.0.get()));
            },
            intervals: vec![],
            before_each: Some(
                (move |counter: &mut Counter, operation: operation::Operation| {
                    assert!(operation.name.contains("add"));
                    before.lock().unwrap().push(("before", counter.0.get()));
                })
                .into(),
            ),
            after_each: Some(
                (move |counter: &mut Counter, _: operation::Operation| {
                    after.lock().unwrap().push(("after", counter.0.get()));
                })
                .into(),
            ),
        },
        options: AktorNewOptions { capacity: 32 },
    };

    let actors = tokio::spawn(aktor_start(AktorSetup {
        actors: setup,
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    }))
    .await
    .unwrap()
    .unwrap();
    let completion = actors.completion();

    assert!(completion.try_report().is_none());
    assert_eq!(add(&actors.handles, 5).await, Ok(5));
    assert_eq!(add(&actors.handles, 0).await, Err("give me something"));

    let (input, mut output) = add::latest(&actors.handles);

    input.send(3);
    assert_eq!(output.next().await, Some(Ok(8)));

    actors.handles.actor.pause().await.unwrap();
    actors
        .handles
        .actor
        .resume(|| Ok::<_, AktorSetupError>(Counter(Rc::new(Cell::new(10)))))
        .await
        .unwrap();

    assert_eq!(add(&actors.handles, 2).await, Ok(12));

    let closing = actors.shutdown();
    let stopping = actors.killswitch().is_stopping();

    drop(closing);

    let report = actors.shutdown().await;

    assert!(
        stopping,
        "shutdown was deferred until its observer was polled"
    );
    assert!(!report.failed(), "{report}");
    assert!(!completion.try_report().unwrap().failed());
    assert_eq!(report.actors[0].actor, "BingoManden");
    assert_eq!(report.actors[0].kind, Some(AktorExecution::TokioThread));
    assert_eq!(
        *log.lock().unwrap(),
        [
            ("before", 0),
            ("after", 5),
            ("before", 5),
            ("after", 5),
            ("before", 5),
            ("after", 8),
            ("end", 8),
            ("before", 10),
            ("after", 12),
            ("end", 12),
        ]
    );
}

#[tokio::test]
async fn multiple() {
    fn counter(
        name: &str,
        fail: bool,
        cleaned: Arc<Notify>,
    ) -> impl setup::AktorStart<setup::closures::AktorCleanupCallback, Group = AktorGroup> {
        AktorNew {
            name: AktorName::new(name),
            role: AktorNoRole,
            kind: AktorKind::TokioThread,
            closures: AktorClosures {
                start: async move || {
                    if fail {
                        Err(AktorSetupError::new("no thanks"))
                    } else {
                        Ok(Counter(Rc::new(Cell::new(0))))
                    }
                },
                end: async move |_: Counter| {
                    cleaned.notify_one();
                },
                intervals: vec![],
                before_each: None,
                after_each: None,
            },
            options: AktorNewOptions { capacity: 32 },
        }
    }

    let cleaned = Arc::new(Notify::new());
    let result = aktor_start(AktorSetup {
        actors: aktor_setups! {
            ready: counter("BingoManden", false, cleaned.clone()),
            failed: counter("Haandboldfuglen", true, Arc::new(Notify::new())),
        },
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await;

    assert!(result.err().unwrap().report.unwrap().startup);
    cleaned.notified().await;

    let actors = tokio::spawn(aktor_start(AktorSetup {
        actors: (
            AktorNew {
                name: AktorName::new(format!("users-{}", 1)),
                role: aktors::Users,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(1_u32),
                    end: (),
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
            AktorNew {
                name: AktorName::new("two"),
                role: aktors::Archive,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(85_u32),
                    end: (),
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
        ),
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    }))
    .await
    .unwrap()
    .unwrap();

    assert_eq!(user_count(&actors.handles.0).await, 1);
    assert_eq!(archive_count::<u32, _>(&actors.handles.1).await, 85);

    let (input, mut output) = user_count::latest(&actors.handles.0);

    input.send();
    assert_eq!(output.next().await, Some(1));
    assert!(!actors.shutdown().await.failed());
}

#[tokio::test]
async fn named() {
    let task = |name: &str| AktorNew {
        name: AktorName::new(name),
        role: aktors::Users,
        kind: AktorKind::TokioTask,
        closures: AktorClosures {
            start: async || Ok::<_, AktorSetupError>(85_u32),
            end: (),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: AktorNewOptions { capacity: 32 },
    };

    let setups = aktor_setups! {
            first: task("first"),
            second: task("second"),
            third: task("third"),
            fourth: task("fourth"),
            fifth: task("fifth"),
            sixth: task("sixth"),
            seventh: task("seventh"),
            eighth: task("eighth"),
            r#type: AktorNew {

                name: AktorName::new("archive"),
                role: aktors::Archive,
                kind: AktorKind::TokioThread,
                closures: AktorClosures {
    start: async || Ok::<_, AktorSetupError>(7_u32),

                    end: (),
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
        };

    let actors = aktor_start(AktorSetup {
        actors: setups,
        shutdown: |_| {},
        options: AktorOptions {
            shutdown_grace: Duration::from_secs(5),
        },
    })
    .await
    .unwrap();

    assert_eq!(user_count(&actors.handles.first).await, 85);
    assert_eq!(user_count(&actors.handles.eighth).await, 85);
    assert_eq!(archive_count::<u32, _>(&actors.handles.r#type).await, 7);

    let report = actors.shutdown().await;

    assert_eq!(report.actors.len(), 9);
    assert!(!report.failed(), "{report}");
}

#[cfg(feature = "std_thread")]
#[test]
fn standard() {
    executor::block_on(async {
        let log = Arc::new(Mutex::new(Vec::new()));
        let before = log.clone();
        let after = log.clone();
        let end = log.clone();
        let owner_thread = Arc::new(Mutex::new(None));
        let started_thread = owner_thread.clone();
        let interval_thread = owner_thread.clone();
        let tick = Arc::new(Notify::new());
        let ping = tick.clone();

        let actors = aktor_start(AktorSetup {
            actors: AktorNew {
                name: AktorName::new("standard counter"),
                role: AktorNoRole,
                kind: AktorKind::StdThread,
                closures: AktorClosures {
                    start: async move || {
                        assert!(tokio::runtime::Handle::try_current().is_err());
                        *started_thread.lock().unwrap() = Some(std::thread::current().id());
                        Ok::<_, AktorSetupError>(Counter(Rc::new(Cell::new(0))))
                    },
                    end: async move |counter: Counter| {
                        assert!(tokio::runtime::Handle::try_current().is_err());
                        assert_eq!(
                            *owner_thread.lock().unwrap(),
                            Some(std::thread::current().id())
                        );
                        end.lock().unwrap().push(("end", counter.0.get()));
                    },
                    intervals: vec![AktorInterval {
                        every: Duration::from_millis(10),
                        run: (async move |_: &mut Counter| {
                            assert!(tokio::runtime::Handle::try_current().is_err());
                            assert_eq!(
                                *interval_thread.lock().unwrap(),
                                Some(std::thread::current().id())
                            );
                            ping.notify_one();
                        })
                        .into(),
                    }],
                    before_each: Some(
                        (move |counter: &mut Counter, _: operation::Operation| {
                            before.lock().unwrap().push(("before", counter.0.get()));
                        })
                        .into(),
                    ),
                    after_each: Some(
                        (move |counter: &mut Counter, _: operation::Operation| {
                            after.lock().unwrap().push(("after", counter.0.get()));
                        })
                        .into(),
                    ),
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

        tokio::select! {
            _ = tick.notified() => {},
            _ = executor::sleep_until(std::time::Instant::now() + Duration::from_secs(2)) => {
                panic!("standard interval did not run");
            },
        }

        assert_eq!(add(&actors.handles, 2).await, Ok(2));
        assert_eq!(add(&actors.handles, 0).await, Err("give me something"));

        let gate = Arc::new(Notify::new());
        let mut reply = held_add::request(&actors.handles, gate.clone())
            .send()
            .await;

        use futures_util::FutureExt;
        assert!(reply.timeout(Duration::MAX).now_or_never().is_none());
        assert!(
            reply
                .timeout(Duration::from_millis(5))
                .await
                .unwrap_err()
                .admitted
        );
        gate.notify_one();
        assert_eq!(reply.await, 3);

        let (input, mut output) = add::latest(&actors.handles);

        input.send(4);
        assert_eq!(output.next().await, Some(Ok(7)));
        input.send(1);

        let report = actors.shutdown().await;

        assert!(!report.failed(), "{report}");
        assert_eq!(report.actors[0].kind, Some(AktorExecution::StdThread));
        assert_eq!(output.next().await, Some(Ok(8)));
        assert_eq!(output.next().await, None);
        assert_eq!(log.lock().unwrap().last(), Some(&("end", 8)));
        drop(input);

        let actors = aktor_start(AktorSetup {
            actors: AktorNew {
                name: AktorName::new("standard cleanup"),
                role: AktorNoRole,
                kind: AktorKind::StdThread,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(0u32),
                    end: async |_: u32| core::future::pending::<()>().await,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: AktorNewOptions { capacity: 32 },
            },
            shutdown: |_| {},
            options: AktorOptions {
                shutdown_grace: Duration::from_millis(200),
            },
        })
        .await
        .unwrap();

        assert!(actors.shutdown().await.timed_out);
    });
}

#[aktor]
async fn held_add(counter: &mut Counter, gate: Arc<Notify>) -> u32 {
    counter.0.set(counter.0.get() + 1);
    gate.notified().await;
    counter.0.get()
}
