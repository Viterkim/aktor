#![cfg(all(feature = "tokio", not(target_family = "wasm")))]

mod ownership;
mod startup;
mod watchdog;
#[cfg(feature = "testing")]
use aktor::testing::call;
use aktor::{AktorGroup, AktorHooks, AktorKind, AktorNew, AktorSetup, AktorStuck, aktor_start};
#[cfg(feature = "testing")]
use er::*;
#[cfg(feature = "testing")]
use std::sync::atomic::AtomicUsize;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

#[path = "../support/mod.rs"]
pub mod support;

#[cfg(feature = "testing")]
#[derive(Er)]
#[er(format = "Flush the settings")]
struct FlushErr;

#[cfg(feature = "testing")]
#[tokio::test]
async fn call_timeout() {
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: support::thread_actor(0usize, 1),
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let actor = started.handles();

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let inside = entered.clone();
    let finish = release.clone();
    let mut running = call(actor, async move |state| {
        inside.notify_one();
        finish.notified().await;
        *state += 1;
        *state
    })
    .send()
    .await
    .unwrap();

    entered.notified().await;

    let timeout = running.timeout(Duration::from_millis(5)).await.unwrap_err();

    assert!(timeout.admitted);

    let queued = call(actor, async |state| *state).send().await.unwrap();
    let timeout = call(actor, async |state| *state += 100)
        .timeout(Duration::from_millis(5))
        .await
        .unwrap_err();

    assert!(!timeout.admitted);

    release.notify_one();
    assert_eq!(running.await, Ok(1));
    assert_eq!(queued.await, Ok(1));

    started.group().stop().await.into_result().unwrap();
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn failure_report() {
    let hooks = Arc::new(AtomicUsize::new(0));
    let hook_count = hooks.clone();
    let after = Arc::new(AtomicBool::new(false));
    let continued = after.clone();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "settings".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || 0usize),
            end: |_| {
                let tree = io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "backup folder is read only",
                )
                .er::<FlushErr>(());

                Err::<(), (FlushErr, String)>(tree.into())
            },
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::new(2).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: move |_| {
            hook_count.fetch_add(1, Ordering::SeqCst);
        },
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let db = started.handles();
    let kill = started.killswitch();
    kill.run(async {
        let value: Result<(), io::Error> =
            call(db, async |_| Err(io::Error::other("ordinary query error")))
                .await
                .unwrap();

        assert!(value.is_err());
        call(db, async |_| -> () { panic!("database died") })
            .await
            .unwrap();
        continued.store(true, Ordering::SeqCst);
    })
    .await;
    let result = started.completion().await.unwrap();

    assert!(
        matches!(&result.failures[0].source, aktor::AktorFailureSource::Actor { name, .. } if name == "settings")
    );

    let report = &result.failures[1].error;

    assert!(report.contains("Flush the settings"));
    assert!(report.contains("backup folder is read only"));
    assert!(!after.load(Ordering::SeqCst));
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn stop_failure() {
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "late failure".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || 0usize),
            end: |_| (),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let kill = started.killswitch();
    let entered = Arc::new(Notify::new());
    let proceed = Arc::new(Notify::new());
    let task_entered = entered.clone();
    let task_proceed = proceed.clone();
    let trigger = tokio::spawn(async move {
        entered.notified().await;
        kill.stop();
        kill.stop();
        proceed.notify_one();
    });

    let db = started.handles();
    started
        .killswitch()
        .run(async {
            let reply = call(db, async move |_| {
                task_entered.notify_one();
                task_proceed.notified().await;
                panic!("crashed after the killswitch");
            })
            .send()
            .await
            .unwrap();

            reply.await.unwrap();
        })
        .await;
    let result = started.completion().await.unwrap();

    trigger.await.unwrap();
    assert!(
        result.failures[0]
            .error
            .contains("crashed after the killswitch")
    );
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn deadline() {
    let Ok(case) = std::env::var("AKTOR_TEST_CASE") else {
        for case in [
            "cleanup",
            "operation",
            "failure",
            #[cfg(feature = "std_thread")]
            "standard cleanup",
        ] {
            let output = support::run_test("deadline", case);

            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .lines()
                    .any(|line| line == "cleanup remains pending")
            );
            assert!(!output.status.success());
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;

                assert_eq!(output.status.signal(), Some(9));
            }
        }

        return;
    };

    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup_started = cleaned.clone();
    let start = aktor::AktorInit::Ready(async || 0usize);
    let end = move |_| {
        let cleanup_started = cleanup_started.clone();

        async move {
            cleanup_started.store(true, Ordering::SeqCst);
            // Blocked in sync code, so its grace can't cut it.
            std::thread::sleep(Duration::from_secs(3600));
        }
    };

    macro_rules! start {
        ($kind:expr) => {
            aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::AbortProcess(()),
                actors: AktorNew {
                    name: "stalled actor".into(),
                    kind: $kind,
                    start,
                    end,
                    hooks: AktorHooks::NONE,
                    capacity: core::num::NonZeroUsize::MIN,
                    stuck: AktorStuck::Kill,
                },
                grace: Duration::from_secs(1),
                shutdown: |_| {},
            })
        };
    }
    #[cfg(feature = "std_thread")]
    let actors = if case == "standard cleanup" {
        start!(AktorKind::StdThread)
    } else {
        start!(AktorKind::TokioThread)
    };
    #[cfg(not(feature = "std_thread"))]
    let mut actors = start!(AktorKind::TokioThread);
    actors.ready().await.unwrap();
    let db = actors.handles();
    let kill = actors.killswitch();
    kill.run(async {
        if case == "operation" {
            let entered = Arc::new(Notify::new());
            let inside = entered.clone();
            let reply = call(db, async move |_| {
                inside.notify_one();
                core::future::pending::<()>().await;
            })
            .send()
            .await
            .unwrap();

            entered.notified().await;
            kill.stop();
            reply.await.unwrap();
        } else if case == "failure" {
            call(db, async |_| panic!("failed before cleanup"))
                .await
                .unwrap();
        } else {
            kill.stop();
            core::future::pending::<()>().await;
        }
    })
    .await;
    let result = actors.completion().await.unwrap();

    assert_eq!(
        result.actors[0].retirement(),
        Some(aktor::AktorRetirement::StillRunning)
    );
    assert!(cleaned.load(Ordering::SeqCst));
    if case == "failure" {
        assert!(
            matches!(&result.failures[0].source, aktor::AktorFailureSource::Actor { name, .. } if name == "stalled actor")
        );
    }

    eprintln!("cleanup remains pending");
    std::thread::sleep(Duration::from_secs(2));
    panic!("shutdown watchdog left cleanup running");
}

#[cfg(feature = "testing")]
struct HoldWake {
    entered: Notify,
    released: std::sync::Mutex<bool>,
    resume: std::sync::Condvar,
}

#[cfg(feature = "testing")]
impl std::task::Wake for HoldWake {
    fn wake(self: Arc<Self>) {
        self.entered.notify_one();

        let mut released = self.released.lock().unwrap();

        while !*released {
            released = self.resume.wait(released).unwrap();
        }
    }
}

#[cfg(feature = "testing")]
struct ResumeWake(Arc<HoldWake>);

#[cfg(feature = "testing")]
impl Drop for ResumeWake {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.resume.notify_one();
    }
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn latest_failure() {
    use aktor::latest::{SendLatest, Session};
    use std::{
        future::Future,
        task::{Context, Waker},
    };

    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "latest search".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || 0u32),
            end: |_| (),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let kill = started.killswitch();
    let wake = Arc::new(HoldWake {
        entered: Notify::new(),
        released: std::sync::Mutex::new(false),
        resume: std::sync::Condvar::new(),
    });

    let resume = ResumeWake(wake.clone());
    let owner = started.handles();
    started
        .killswitch()
        .run(async move {
            let release = Arc::new(Notify::new());
            let inside = release.clone();
            let (sender, mut results) = owner.session(
                aktor::operation::Operation {
                    name: "failing search",
                    caller: std::panic::Location::caller(),
                },
                async move |_: &mut u32, ()| {
                    inside.notified().await;
                    panic!("search handler exploded");
                },
            );

            sender.send(());

            let waker = Waker::from(wake.clone());
            let next = results.next();

            tokio::pin!(next);
            assert!(
                next.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            release.notify_one();
            wake.entered.notified().await;

            // The actor is held in close(), before its panic collector can run.
            assert!(
                next.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(!kill.is_stopping());
            drop(resume);
            next.await;
        })
        .await;
    let result = started.completion().await.unwrap();

    assert!(result.failures[0].error.contains("search handler exploded"));

    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "latest output".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async move || 0usize),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let closing = started.completion();
    let owner = started.handles();
    let (sender, mut results) = owner.session(
        aktor::operation::Operation {
            name: "failed output",
            caller: std::panic::Location::caller(),
        },
        async |_: &mut usize, ()| -> usize { panic!("original latest failure") },
    );

    sender.send(());

    let error = results.next().await.unwrap().unwrap_err();
    assert!(results.next().await.is_none());

    let failure = &closing.wait().await.unwrap().failures[0];
    assert_eq!(error.failure_id, Some(failure.id));
    assert!(error.report.contains("original latest failure"));
}

#[tokio::test]
async fn cleanup_failure() {
    let hook = Arc::new(AtomicBool::new(false));
    let completed = hook.clone();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: (
            AktorNew {
                name: "storage".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || ()),
                end: |_| Err::<(), _>(((), String::from("could not flush"))),
                hooks: AktorHooks::NONE,
                capacity: core::num::NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "audio".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async move || 17usize),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: core::num::NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: move |_| {
            completed.store(true, Ordering::SeqCst);
        },
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let kill = started.killswitch();
    let closing = started.completion();
    let (actor, sibling) = started.handles();

    drop(actor.shutdown());
    tokio::time::timeout(Duration::from_secs(1), kill.wait_stopping())
        .await
        .unwrap();

    let report = closing.wait().await.unwrap();

    assert!(hook.load(Ordering::SeqCst));
    assert_eq!(report.to_string().matches("could not flush").count(), 1);
    let failure = &report.failures[0];
    assert!(
        matches!(&failure.source, aktor::AktorFailureSource::Actor { name, .. } if name == "storage")
    );
    assert!(sibling.completion().wait().await.is_ok());

    let (error, report) = actor.completion().wait().await.unwrap_err();
    let aktor::owner::OwnerError::Cleanup = &*error else {
        panic!("typed cleanup error was lost");
    };

    assert!(report.contains("could not flush"));
}
