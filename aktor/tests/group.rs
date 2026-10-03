#![cfg(all(feature = "tokio", not(target_family = "wasm")))]

use aktor::{
    ActorArgs, AktorError, AktorGroup,
    message::{call, call_async},
};
use er::*;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Er)]
#[er(format = "Flush the settings")]
struct FlushErr;

struct CleanupData(u32);

#[tokio::test]
async fn typed_cleanup_and_timeouts() {
    let result = AktorGroup::new()
        .run(
            async |app| {
                let actor = app
                    .spawn(ActorArgs {
                        name: "typed cleanup".into(),
                        capacity: 1,
                        setup: || Ok::<_, AktorError>(0usize),
                        cleanup: |_| {
                            Err(AktorError {
                                diagnostics: "could not flush".into(),
                                data: CleanupData(17),
                            })
                        },
                    })
                    .await
                    .unwrap();

                let started = Arc::new(Notify::new());
                let release = Arc::new(Notify::new());
                let inside = started.clone();
                let finish = release.clone();
                let mut running = call_async(
                    &actor.handle,
                    async move |state, ()| {
                        inside.notify_one();
                        finish.notified().await;
                        *state += 1;
                        *state
                    },
                    (),
                )
                .send()
                .await;
                started.notified().await;

                let timeout = running.timeout(Duration::from_millis(5)).await.unwrap_err();
                assert!(timeout.admitted);
                let queued = call(&actor.handle, |state, ()| *state, ()).send().await;
                let timeout = call(&actor.handle, |state, ()| *state += 100, ())
                    .timeout(Duration::from_millis(5))
                    .await
                    .unwrap_err();
                assert!(!timeout.admitted);

                release.notify_one();
                assert_eq!(running.await, 1);
                assert_eq!(queued.await, 1);
                let completion = actor.shutdown();
                assert!(matches!(
                    call(&actor.handle, |_, ()| (), ()).try_send(),
                    Err(aktor::message::TrySendError::Closed(_))
                ));
                let error = (&completion).await.unwrap_err();
                let aktor::owner::OwnerError::Cleanup(errors) = &*error else {
                    panic!("cleanup error was lost");
                };
                assert_eq!(errors.errors[0].data.0, 17);
                assert_eq!(completion.await.unwrap_err().to_string(), error.to_string());
                Ok::<_, AktorError>(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .unwrap_err();

    assert_eq!(
        result.actors[0].diagnostics[0].diagnostics,
        "could not flush"
    );
}

#[tokio::test]
async fn failure_and_cleanup_reports() {
    let group = AktorGroup::with_grace(Duration::from_secs(1));
    let finished = group.completion();
    let hooks = Arc::new(AtomicUsize::new(0));
    let hook_count = hooks.clone();
    let after = Arc::new(AtomicBool::new(false));
    let continued = after.clone();
    let result = group
        .run(
            async |app| {
                let db = app
                    .spawn(ActorArgs {
                        name: "settings".into(),
                        capacity: 2,
                        setup: || Ok::<_, AktorError>(0usize),
                        cleanup: |_| {
                            Err(io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "backup folder is read only",
                            ))
                            .er::<FlushErr>(())
                            .er_report_string()
                            .map_err(|diagnostics| AktorError {
                                diagnostics,
                                data: (),
                            })
                        },
                    })
                    .await
                    .unwrap();
                let value: Result<(), io::Error> = call(
                    &db.handle,
                    |_, ()| Err(io::Error::other("ordinary query error")),
                    (),
                )
                .await;
                assert!(value.is_err());
                call(&db.handle, |_, ()| -> () { panic!("database died") }, ()).await;
                continued.store(true, Ordering::SeqCst);
                Ok::<_, AktorError>(())
            },
            async move |report| {
                hook_count.fetch_add(1, Ordering::SeqCst);
                assert_eq!(report.actors.len(), 1);
                Ok::<_, AktorError>(())
            },
        )
        .await
        .unwrap_err();

    assert_eq!(result.failure.as_ref().unwrap().actor, "settings");
    let report = result.actors[0].diagnostics[0].diagnostics.clone();
    assert!(report.contains("Flush the settings"));
    assert!(report.contains("backup folder is read only"));
    assert!(!after.load(Ordering::SeqCst));
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
    assert_eq!(finished.wait().await.to_string(), result.to_string());
}

#[tokio::test]
async fn killswitch_then_failure() {
    let group = AktorGroup::with_grace(Duration::from_secs(1));
    let kill = group.killswitch();
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
    let result = group
        .run(
            async |app| {
                let db = app
                    .spawn(ActorArgs {
                        name: "late failure".into(),
                        capacity: 1,
                        setup: || Ok::<_, AktorError>(0usize),
                        cleanup: |_| Ok::<_, AktorError>(()),
                    })
                    .await
                    .unwrap();
                let reply = call_async(
                    &db.handle,
                    async move |_, ()| {
                        task_entered.notify_one();
                        task_proceed.notified().await;
                        panic!("crashed after the killswitch");
                    },
                    (),
                )
                .send()
                .await;
                reply.await;
                Ok::<_, AktorError>(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .unwrap_err();
    trigger.await.unwrap();
    assert!(
        result
            .failure
            .unwrap()
            .message
            .contains("crashed after the killswitch")
    );
}

#[tokio::test]
async fn stuck_work_and_hooks_are_bounded() {
    for operation in [false, true] {
        let group = AktorGroup::with_grace(Duration::from_millis(200));
        let kill = group.killswitch();
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleanup_started = cleaned.clone();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            group.run(
                async |app| {
                    let db = app
                        .spawn_async(ActorArgs {
                            name: "stalled actor".into(),
                            capacity: 1,
                            setup: async || Ok::<_, AktorError>(0usize),
                            cleanup: move |_| {
                                let cleanup_started = cleanup_started.clone();
                                async move {
                                    cleanup_started.store(true, Ordering::SeqCst);
                                    core::future::pending::<()>().await;
                                    Ok::<_, AktorError>(())
                                }
                            },
                        })
                        .await
                        .unwrap();
                    if operation {
                        let entered = Arc::new(Notify::new());
                        let inside = entered.clone();
                        let reply = call_async(
                            &db.handle,
                            async move |_, ()| {
                                inside.notify_one();
                                core::future::pending::<()>().await;
                            },
                            (),
                        )
                        .send()
                        .await;
                        entered.notified().await;
                        kill.stop();
                        reply.await;
                    } else {
                        kill.stop();
                        core::future::pending::<()>().await;
                    }
                    Ok::<_, AktorError>(())
                },
                async |_| {
                    core::future::pending::<()>().await;
                    Ok::<_, AktorError>(())
                },
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(result.timed_out);
        assert!(result.actors[0].timed_out);
        assert_eq!(cleaned.load(Ordering::SeqCst), !operation);
    }
}

#[tokio::test]
async fn failure_starts_before_stuck_cleanup() {
    let group = AktorGroup::with_grace(Duration::from_millis(200));
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        group.run(
            async |app| {
                let db = app
                    .spawn_async(ActorArgs {
                        name: "early failure".into(),
                        capacity: 1,
                        setup: async || Ok::<_, AktorError>(0usize),
                        cleanup: async |_| {
                            core::future::pending::<()>().await;
                            Ok::<_, AktorError>(())
                        },
                    })
                    .await
                    .unwrap();
                call(
                    &db.handle,
                    |_, ()| -> () { panic!("failed before cleanup") },
                    (),
                )
                .await;
                Ok::<_, AktorError>(())
            },
            async |_| Ok::<_, AktorError>(()),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(result.failure.unwrap().actor, "early failure");
}

#[tokio::test]
async fn paused_calls_resume() {
    let result = AktorGroup::new()
        .run(
            async |app| {
                let database = app
                    .spawn(ActorArgs {
                        name: "paused database".into(),
                        capacity: 1,
                        setup: || Ok::<_, AktorError>(7usize),
                        cleanup: |_| Ok::<_, AktorError>(()),
                    })
                    .await
                    .unwrap();
                database.actor.pause().await.unwrap();
                let request = call(&database.handle, |value, ()| *value, ());
                tokio::pin!(request);
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), &mut request)
                        .await
                        .is_err()
                );
                database
                    .actor
                    .resume(|| {
                        Err(AktorError {
                            diagnostics: "opening failed".into(),
                            data: (),
                        })
                    })
                    .await
                    .unwrap_err();
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), &mut request)
                        .await
                        .is_err()
                );
                database.actor.resume(|| Ok(19usize)).await.unwrap();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(1), &mut request)
                        .await
                        .unwrap(),
                    19
                );
                Ok::<_, AktorError>(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await;
    assert!(result.is_ok());
}

#[test]
fn blocking_shutdown_exits() {
    if let Ok(mode) = std::env::var("AKTOR_GROUP_BLOCKING_PROOF") {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let group = AktorGroup::with_grace(Duration::from_millis(200));
            let kill = group.killswitch();
            let actor_blocks = mode == "actor";
            let setup_blocks = mode == "setup";
            if setup_blocks {
                let kill = kill.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(40));
                    kill.stop();
                });
            }
            let _report = group
                .run(
                    async |app| {
                        let _actor = app
                            .spawn(ActorArgs {
                                name: "blocking cleanup".into(),
                                capacity: 1,
                                setup: move || {
                                    if setup_blocks {
                                        loop {
                                            std::thread::sleep(Duration::from_secs(10));
                                        }
                                    }
                                    Ok::<_, AktorError>(())
                                },
                                cleanup: move |_| {
                                    if actor_blocks {
                                        loop {
                                            std::thread::sleep(Duration::from_secs(10));
                                        }
                                    }
                                    Ok::<_, AktorError>(())
                                },
                            })
                            .await
                            .unwrap();
                        kill.stop();
                        core::future::pending::<()>().await;
                        Ok::<_, AktorError>(())
                    },
                    async |_| {
                        if !actor_blocks && !setup_blocks {
                            loop {
                                std::thread::sleep(Duration::from_secs(10));
                            }
                        }
                        Ok::<_, AktorError>(())
                    },
                )
                .await;
            // An uninterruptible actor must still cause exit if the report was ignored.
            std::thread::sleep(Duration::from_secs(10));
        });
        return;
    }

    for mode in ["actor", "application", "setup"] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "blocking_shutdown_exits", "--nocapture"])
            .env("AKTOR_GROUP_BLOCKING_PROOF", mode)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                panic!("shutdown watchdog did not stop blocking {mode}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr).contains("Shutdown deadline reached"));
    }
}

#[tokio::test]
async fn startup_error_closes_existing_actors() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let closed = cleaned.clone();
    let result = AktorGroup::new()
        .run(
            async |app| -> Result<(), AktorError> {
                let _first = app
                    .spawn(ActorArgs {
                        name: "opened resource".into(),
                        capacity: 1,
                        setup: || Ok::<_, AktorError>(()),
                        cleanup: move |_| {
                            closed.store(true, Ordering::SeqCst);
                            Ok::<_, AktorError>(())
                        },
                    })
                    .await
                    .map_err(|error| AktorError {
                        diagnostics: error.to_string(),
                        data: (),
                    })?;
                let _second = app
                    .spawn(ActorArgs {
                        name: "failed setup".into(),
                        capacity: 1,
                        setup: || {
                            Err::<(), _>(AktorError {
                                diagnostics: "setup went wrong".into(),
                                data: (),
                            })
                        },
                        cleanup: |_| Ok::<_, AktorError>(()),
                    })
                    .await
                    .map_err(|error| AktorError {
                        diagnostics: error.to_string(),
                        data: (),
                    })?;
                Ok(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .unwrap_err();
    assert!(cleaned.load(Ordering::SeqCst));
    assert_eq!(result.actors.len(), 2);
    assert_eq!(result.failure.as_ref().unwrap().actor, "failed setup");
    assert_eq!(result.failure.as_ref().unwrap().phase, "setup");
    assert!(result.actors.iter().any(|actor| {
        actor
            .diagnostics
            .iter()
            .any(|error| error.diagnostics.contains("setup went wrong"))
    }));
}

struct ReleaseOnDrop(Arc<Notify>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn cancelled_startup_keeps_errors() {
    for setup_fails in [true, false] {
        let group = AktorGroup::with_grace(Duration::from_secs(1));
        let kill = group.killswitch();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let inside = entered.clone();
        let setup_release = release.clone();
        let trigger = tokio::spawn(async move {
            entered.notified().await;
            kill.stop();
        });

        let result = group
            .run(
                async move |app| -> Result<(), AktorError> {
                    let _release = ReleaseOnDrop(release);
                    let _owner = app
                        .spawn_async(ActorArgs {
                            name: "cancelled startup".into(),
                            capacity: 1,
                            setup: async move || {
                                inside.notify_one();
                                setup_release.notified().await;
                                if setup_fails {
                                    Err(AktorError::new("late setup error"))
                                } else {
                                    Ok(())
                                }
                            },
                            cleanup: async |_| Err(AktorError::new("late cleanup error")),
                        })
                        .await
                        .unwrap();
                    Ok(())
                },
                async |_| Ok::<_, AktorError>(()),
            )
            .await
            .unwrap_err();
        trigger.await.unwrap();

        assert!(!result.timed_out);
        let expected = if setup_fails {
            "late setup error"
        } else {
            "late cleanup error"
        };
        assert!(
            result.actors[0]
                .diagnostics
                .iter()
                .any(|error| error.diagnostics.contains(expected))
        );
    }
}

struct BrokenDrop;
impl Drop for BrokenDrop {
    fn drop(&mut self) {
        panic!("application destructor failed");
    }
}

#[tokio::test]
async fn cancellation_keeps_shutdown_running() {
    for hook in [false, true] {
        let group = AktorGroup::with_grace(Duration::from_millis(200));
        let kill = group.killswitch();
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleanup = cleaned.clone();
        let result = group
            .run(
                async move |app| -> Result<(), AktorError> {
                    let _owner = app
                        .spawn(ActorArgs {
                            name: "sibling".into(),
                            capacity: 1,
                            setup: || Ok::<_, AktorError>(()),
                            cleanup: move |_| {
                                cleanup.store(true, Ordering::SeqCst);
                                Ok::<_, AktorError>(())
                            },
                        })
                        .await
                        .unwrap();
                    let _drop = (!hook).then(|| BrokenDrop);
                    kill.stop();
                    core::future::pending().await
                },
                async move |_| {
                    let _drop = hook.then(|| BrokenDrop);
                    if hook {
                        core::future::pending::<()>().await;
                    }
                    Ok::<_, AktorError>(())
                },
            )
            .await
            .unwrap_err();

        assert!(cleaned.load(Ordering::SeqCst));
        assert!(
            result
                .failure
                .unwrap()
                .message
                .contains("application destructor failed")
        );
    }
}

struct HoldWake {
    entered: Notify,
    released: std::sync::Mutex<bool>,
    resume: std::sync::Condvar,
}
impl std::task::Wake for HoldWake {
    fn wake(self: Arc<Self>) {
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.resume.wait(released).unwrap();
        }
    }
}
struct ResumeWake(Arc<HoldWake>);
impl Drop for ResumeWake {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.resume.notify_one();
    }
}

#[tokio::test]
async fn latest_keeps_the_original_failure() {
    use aktor::latest::{SendLatest, Session};
    use std::{
        future::Future,
        task::{Context, Waker},
    };

    let group = AktorGroup::new();
    let kill = group.killswitch();
    let wake = Arc::new(HoldWake {
        entered: Notify::new(),
        released: std::sync::Mutex::new(false),
        resume: std::sync::Condvar::new(),
    });
    let resume = ResumeWake(wake.clone());
    let result = group
        .run(
            async move |app| -> Result<(), AktorError> {
                let owner = app
                    .spawn(ActorArgs {
                        name: "latest search".into(),
                        capacity: 1,
                        setup: || Ok::<_, AktorError>(0u32),
                        cleanup: |_| Ok::<_, AktorError>(()),
                    })
                    .await
                    .unwrap();
                let release = Arc::new(Notify::new());
                let inside = release.clone();
                let (sender, mut results) = (&owner.handle).session(
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
                Ok(())
            },
            async |_| Ok::<_, AktorError>(()),
        )
        .await
        .unwrap_err();
    assert!(
        result
            .failure
            .unwrap()
            .message
            .contains("search handler exploded")
    );
}
