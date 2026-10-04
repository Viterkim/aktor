#![cfg(all(feature = "tokio", not(target_family = "wasm")))]

use aktor::{
    ActorArgs, AktorCleanupError, AktorError, AktorGroup,
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

#[path = "support/mod.rs"]
pub mod support;

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

struct StartupDrop(Arc<AtomicBool>);
impl Drop for StartupDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn blocking_shutdown_exits() {
    if let Ok(mode) = std::env::var("AKTOR_GROUP_BLOCKING_PROOF") {
        if mode == "exit handler" {
            unsafe extern "C" {
                fn atexit(callback: extern "C" fn()) -> core::ffi::c_int;
            }
            extern "C" fn wait_forever() {
                loop {
                    std::thread::sleep(Duration::from_secs(10));
                }
            }

            // The callback has static lifetime and the C calling convention.
            assert_eq!(unsafe { atexit(wait_forever) }, 0);
        }
        #[cfg(unix)]
        if mode == "abort handler" {
            unsafe extern "C" {
                fn signal(
                    number: core::ffi::c_int,
                    handler: extern "C" fn(core::ffi::c_int),
                ) -> usize;
            }
            extern "C" fn wait_forever(_: core::ffi::c_int) {
                loop {
                    std::hint::spin_loop();
                }
            }

            // The handler only spins, without calling anything unsafe in a signal.
            assert_ne!(unsafe { signal(6, wait_forever) }, usize::MAX);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        if mode == "stderr failure" {
            std::panic::set_hook(Box::new(|_| {}));
            runtime.block_on(async {
                let mut group = AktorGroup::with_grace(Duration::from_millis(200));
                let closing = group.start().unwrap();
                let actor = group.spawn_value("reporting", 0usize).await.unwrap();
                let (ready, acquired) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let stderr = std::io::stderr();
                    let _locked = stderr.lock();
                    ready.send(()).unwrap();
                    std::thread::sleep(Duration::from_secs(10));
                });
                acquired.recv().unwrap();

                aktor::message::call(&actor.handle, |_, ()| panic!("report failure"), ())
                    .cast()
                    .await;
                let report = closing.wait().await;
                assert!(report.failure.unwrap().message.contains("report failure"));
                assert!(!report.timed_out);
            });
            return;
        }
        if mode == "pending setup" {
            runtime.block_on(async {
                let mut group = AktorGroup::with_grace(Duration::from_millis(200));
                let kill = group.killswitch();
                let closing = group.start_with(async |_| Ok::<_, AktorError>(())).unwrap();
                let entered = Arc::new(Notify::new());
                let inside = entered.clone();
                let dropped = Arc::new(AtomicBool::new(false));
                let cleanup = dropped.clone();
                let startup = tokio::spawn(async move {
                    group
                        .spawn_async(ActorArgs::new(
                            "pending setup",
                            async move || {
                                let _drop = StartupDrop(cleanup);
                                inside.notify_one();
                                core::future::pending::<()>().await;
                                Ok::<_, AktorError>(())
                            },
                            async |_| Ok::<_, AktorError>(()),
                        ))
                        .await
                });
                entered.notified().await;
                kill.stop();
                let report = closing.wait().await;
                assert!(dropped.load(Ordering::SeqCst));
                assert!(report.timed_out);
                assert!(report.failure.is_none());
                assert!(matches!(
                    startup.await.unwrap(),
                    Err(aktor::listener::DedicatedStartError::Closed)
                ));
                tokio::time::sleep(Duration::from_millis(250)).await;
            });
            return;
        }
        runtime.block_on(async {
            let group = AktorGroup::with_grace(Duration::from_millis(200));
            let kill = group.killswitch();
            if mode == "stderr" {
                let (ready, acquired) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let stderr = std::io::stderr();
                    let _locked = stderr.lock();
                    ready.send(()).unwrap();
                    std::thread::sleep(Duration::from_secs(10));
                });
                acquired.recv().unwrap();
            }
            let actor_blocks = matches!(
                &*mode,
                "actor" | "stderr" | "exit handler" | "abort handler"
            );
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

    for mode in [
        "actor",
        "application",
        "setup",
        "pending setup",
        "stderr",
        "stderr failure",
        "exit handler",
        #[cfg(unix)]
        "abort handler",
    ] {
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
                child.wait().unwrap();
                panic!("shutdown watchdog did not stop blocking {mode}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        let forced_exit = !matches!(mode, "pending setup" | "stderr failure");
        if forced_exit {
            assert!(!output.status.success());
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(output.status.signal(), Some(9));
            }
        } else {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if !forced_exit {
            assert!(!String::from_utf8_lossy(&output.stderr).contains("Shutdown deadline reached"));
        }
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

    let mut actors = AktorGroup::new();
    let closing = actors.start().unwrap();
    let owner = actors.spawn_value("latest output", 0usize).await.unwrap();
    let (sender, mut results) = (&owner.handle).session(
        aktor::operation::Operation {
            name: "failed output",
            caller: std::panic::Location::caller(),
        },
        async |_: &mut usize, ()| -> usize { panic!("original latest failure") },
    );
    sender.send(());
    let report = closing.wait().await;

    for _ in 0..4 {
        let mut next = Box::pin(results.next());
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let failure = report.failure.unwrap().message;
    assert!(failure.contains("original latest failure"));
    assert_eq!(closing.wait().await.failure.unwrap().message, failure);
}

#[tokio::test]
async fn ordinary_startup_and_shutdown() {
    for fail in [false, true] {
        let mut actors = AktorGroup::new();
        let kill = actors.killswitch();
        let completed = actors.completion();
        let hooks = Arc::new(AtomicUsize::new(0));
        let hook_count = hooks.clone();
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleanup = cleaned.clone();
        let closing = actors
            .start_with(async move |report| {
                assert_eq!(report.actors.len(), 1);
                hook_count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, AktorError>(())
            })
            .unwrap();
        assert!(
            actors
                .start_with(async |_| Ok::<_, AktorError>(()))
                .is_err()
        );

        let setup = || Ok::<_, AktorError>(0usize);
        let cleanup = move |_| {
            cleanup.store(true, Ordering::SeqCst);
            Ok::<_, AktorError>(())
        };
        let database = actors
            .spawn(ActorArgs::new("database", setup, cleanup))
            .await
            .unwrap();
        let caller_kill = kill.clone();
        let started = Arc::new(Notify::new());
        let caller_started = started.clone();
        let caller = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = caller_kill.wait_stopping() => {},
                _ = async {
                    let result = call(&database.handle, |_, ()| Err::<(), _>("query failed"), ()).await;
                    assert_eq!(result, Err("query failed"));
                    assert!(!caller_kill.is_stopping());
                    if fail {
                        caller_started.notify_one();
                        call(&database.handle, |_, ()| panic!("storage died"), ()).await;
                    } else {
                        database.actor.pause().await.unwrap();
                        caller_started.notify_one();
                        call(&database.handle, |_, ()| (), ()).await;
                    }
                } => panic!("caller continued after permanent loss"),
            }
        });
        started.notified().await;
        if !fail {
            kill.stop();
        }
        let report = tokio::time::timeout(Duration::from_secs(2), closing.wait())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), caller)
            .await
            .unwrap()
            .unwrap();
        assert!(cleaned.load(Ordering::SeqCst));
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        assert_eq!(report.failed(), fail);
        assert_eq!(completed.wait().await.to_string(), report.to_string());
        if fail {
            assert!(
                report
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("storage died")
            );
        }
        assert!(matches!(
            actors
                .spawn(ActorArgs::new(
                    "late",
                    || Ok::<_, AktorError>(()),
                    |_| Ok::<_, AktorError>(())
                ))
                .await,
            Err(aktor::listener::DedicatedStartError::Closed)
        ));
    }
}

#[tokio::test]
async fn default_group() {
    let mut actors = AktorGroup::new();
    let closing = actors.start().unwrap();
    let actor = actors.spawn_value("counter", 17usize).await.unwrap();
    let shutdown = actors.shutdown();
    assert!(actors.killswitch().is_stopping());
    assert!(!(&closing).await.failed());
    assert_eq!(closing.await.to_string(), shutdown.await.to_string());
    assert!(actor.completion().wait().await.is_ok());

    let count = std::rc::Rc::new(std::cell::Cell::new(0));
    AktorGroup::new()
        .run(
            async |_| Ok::<_, AktorError>(()),
            async |_| {
                count.set(count.get() + 1);
                Ok::<_, AktorError>(())
            },
        )
        .await
        .unwrap();
    assert_eq!(count.get(), 1);
}

#[tokio::test]
async fn application_creation() {
    use futures_util::FutureExt;
    use std::{future::Ready, panic::AssertUnwindSafe};

    fn application(_: &mut AktorGroup) -> Ready<Result<(), AktorError>> {
        panic!("application creation failed");
    }

    let group = AktorGroup::new();
    let completed = group.completion();
    let cleanup = Arc::new(AtomicBool::new(false));
    let cleaned = cleanup.clone();
    let result = AssertUnwindSafe(group.run(application, async move |_| {
        cleaned.store(true, Ordering::SeqCst);
        Ok::<_, AktorError>(())
    }))
    .catch_unwind()
    .await;
    let report = completed.wait().await;

    assert!(result.is_ok(), "application creation escaped run()");
    assert_eq!(
        report.failure.unwrap().message,
        "application creation failed"
    );
    assert!(cleanup.load(Ordering::SeqCst));
}

#[tokio::test]
async fn ordinary_startup_failure_closes_siblings() {
    let mut actors = AktorGroup::new();
    let hooks = Arc::new(AtomicBool::new(false));
    let hook = hooks.clone();
    let closing = actors
        .start_with(async move |_| {
            hook.store(true, Ordering::SeqCst);
            Ok::<_, AktorError>(())
        })
        .unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup = cleaned.clone();
    let sibling = actors
        .spawn(ActorArgs::new(
            "audio",
            || Ok::<_, AktorError>(()),
            move |_| {
                cleanup.store(true, Ordering::SeqCst);
                Ok::<_, AktorError>(())
            },
        ))
        .await
        .unwrap();
    let startup = tokio::spawn(async move {
        actors
            .spawn(ActorArgs::new(
                "storage",
                || {
                    Err::<(), _>(AktorError {
                        diagnostics: "could not open".into(),
                        data: 23u32,
                    })
                },
                |_| Ok::<_, AktorError>(()),
            ))
            .await?;
        Ok::<_, aktor::listener::DedicatedStartError<AktorError<u32>>>(())
    });
    let failed = tokio::time::timeout(Duration::from_secs(2), startup)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    let aktor::listener::DedicatedStartError::Init(error) = failed else {
        panic!("typed setup error was lost");
    };
    assert_eq!(error.data, 23);
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(hooks.load(Ordering::SeqCst));
    let report = closing.wait().await;
    assert_eq!(report.failure.unwrap().actor, "storage");
    assert_eq!(report.actors.len(), 2);
    assert!(sibling.completion().wait().await.is_ok());
}

#[tokio::test]
async fn listener_retains_cancelled_startup() {
    for setup_fails in [false, true] {
        let mut actors = AktorGroup::new();
        let kill = actors.killswitch();
        let closing = actors
            .start_with(async |_| Ok::<_, AktorError>(()))
            .unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let setup_entered = entered.clone();
        let setup_release = release.clone();
        let startup = tokio::spawn(async move {
            actors
                .spawn_async(ActorArgs::new(
                    "storage",
                    move || async move {
                        setup_entered.notify_one();
                        setup_release.notified().await;
                        if setup_fails {
                            Err(AktorError::new("late setup failed"))
                        } else {
                            Ok(())
                        }
                    },
                    async |_| Err::<(), _>(AktorError::new("late cleanup failed")),
                ))
                .await
        });
        entered.notified().await;
        kill.stop();
        startup.abort();
        assert!(startup.await.err().unwrap().is_cancelled());
        release.notify_one();
        let report = tokio::time::timeout(Duration::from_secs(2), closing.wait())
            .await
            .unwrap();
        let expected = if setup_fails {
            "late setup failed"
        } else {
            "late cleanup failed"
        };
        assert!(report.failed());
        assert!(report.actors.iter().any(|actor| {
            actor
                .diagnostics
                .iter()
                .any(|error| error.diagnostics.contains(expected))
        }));
    }
}

#[tokio::test]
async fn plain_value_and_cancelled_completion() {
    let mut actors = AktorGroup::new();
    assert!(matches!(
        actors.spawn_value("early", 0usize).await,
        Err(aktor::listener::DedicatedStartError::NotStarted)
    ));
    let kill = actors.killswitch();
    let completed = actors.completion();
    let closing = actors
        .start_with(async |_| {
            Err::<(), _>(AktorCleanupError {
                diagnostics: "application cleanup error".into(),
                data: std::rc::Rc::new(17u32),
            })
        })
        .unwrap();
    let waiter = tokio::spawn(async move { closing.wait().await });
    tokio::task::yield_now().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());

    let state = actors.spawn_value("counter", 7usize).await.unwrap();
    assert_eq!(call(&state.handle, |state, ()| *state, ()).await, 7);
    kill.stop();
    let report = tokio::time::timeout(Duration::from_secs(2), completed.wait())
        .await
        .unwrap();
    assert_eq!(report.actors.len(), 1);
    assert_eq!(
        report.application[0].diagnostics,
        "application cleanup error"
    );
}

#[tokio::test]
async fn dropping_group_closes_actors() {
    for return_error in [false, true] {
        let mut actors = AktorGroup::new();
        let kill = actors.killswitch();
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleanup = cleaned.clone();
        let closing = actors
            .start_with(async |_| Ok::<_, AktorError>(()))
            .unwrap();
        let actor = actors
            .spawn(ActorArgs::new(
                "owned actor",
                || Ok::<_, AktorError>(()),
                move |_| {
                    cleanup.store(true, Ordering::SeqCst);
                    Ok::<_, AktorError>(())
                },
            ))
            .await
            .unwrap();

        if return_error {
            let startup = async move {
                let _actors = actors;
                Err::<(), _>(io::Error::other("application setup failed"))?;
                Ok::<_, io::Error>(())
            };
            assert!(startup.await.is_err());
        } else {
            drop(actors);
        }

        assert!(kill.is_stopping());
        let report = tokio::time::timeout(Duration::from_secs(1), closing.wait())
            .await
            .unwrap();
        assert!(!report.failed());
        assert!(cleaned.load(Ordering::SeqCst));
        assert!(actor.completion().wait().await.is_ok());
    }
}

#[tokio::test]
async fn terminal_cleanup_closes_group() {
    let mut actors = AktorGroup::new();
    let kill = actors.killswitch();
    let hook = Arc::new(AtomicBool::new(false));
    let completed = hook.clone();
    let closing = actors
        .start_with(async move |_| {
            completed.store(true, Ordering::SeqCst);
            Ok::<_, AktorCleanupError>(())
        })
        .unwrap();
    let actor = actors
        .spawn(ActorArgs::new(
            "storage",
            || Ok::<_, AktorError>(()),
            |_| {
                Err::<(), _>(AktorCleanupError {
                    diagnostics: "could not flush".into(),
                    data: CleanupData(31),
                })
            },
        ))
        .await
        .unwrap();
    let sibling = actors.spawn_value("audio", 17usize).await.unwrap();

    drop(actor.shutdown());
    tokio::time::timeout(Duration::from_secs(1), kill.wait_stopping())
        .await
        .unwrap();
    let report = closing.wait().await;
    assert!(hook.load(Ordering::SeqCst));
    assert_eq!(report.failure.unwrap().actor, "storage");
    assert!(
        report.actors.iter().any(|actor| actor.actor == "storage"
            && actor.diagnostics[0].diagnostics == "could not flush")
    );
    assert!(sibling.completion().wait().await.is_ok());
    let error = actor.completion().wait().await.unwrap_err();
    let aktor::owner::OwnerError::Cleanup(errors) = &*error else {
        panic!("typed cleanup error was lost");
    };
    assert_eq!(errors.errors[0].data.0, 31);
}

struct StopWake {
    kill: aktor::KillSwitch,
    actor: Option<aktor::listener::Actor<(), AktorError, AktorError>>,
}
impl std::task::Wake for StopWake {
    fn wake(self: Arc<Self>) {
        if let Some(actor) = &self.actor {
            actor.close_admission();
        } else {
            self.kill.stop();
        }
    }
}

#[test]
fn shutdown_ownership() {
    if let Ok(mode) = std::env::var("AKTOR_CHILD") {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let group = AktorGroup::with_grace(Duration::from_millis(500));
                let kill = group.killswitch();
                let completed = group.completion();

                let ready = Arc::new(Notify::new());
                let entered = ready.clone();
                let cleaned = Arc::new(AtomicBool::new(false));
                let cleanup = cleaned.clone();

                let actor_ready = Arc::new(Notify::new());
                let actor_entered = actor_ready.clone();
                let release = Arc::new(Notify::new());
                let actor_release = release.clone();
                let actor_mode = mode.clone();

                let hook_ready = Arc::new(Notify::new());
                let hook_entered = hook_ready.clone();
                let app_mode = mode.clone();
                let hook_mode = mode.clone();
                let stopping = kill.clone();

                let task = tokio::spawn(group.run(
                    async move |app| -> Result<(), AktorError<BrokenDrop>> {
                        let _actor = app
                            .spawn_async(ActorArgs::new(
                                "resource",
                                async || Ok::<_, AktorError>(()),
                                move |_| {
                                    let waiting = actor_mode == "actor driver";
                                    let entered = actor_entered.clone();
                                    let release = actor_release.clone();
                                    let cleanup = cleanup.clone();
                                    async move {
                                        if waiting {
                                            entered.notify_one();
                                            release.notified().await;
                                        }
                                        cleanup.store(true, Ordering::SeqCst);
                                        Ok::<_, AktorError>(())
                                    }
                                },
                            ))
                            .await
                            .unwrap();

                        entered.notify_one();
                        if app_mode == "application payload" {
                            std::panic::panic_any(BrokenDrop);
                        }
                        if app_mode == "application error" {
                            return Err(AktorError {
                                diagnostics: "original application error".into(),
                                data: BrokenDrop,
                            });
                        }
                        if app_mode == "driver" {
                            core::future::pending::<()>().await;
                        }
                        stopping.stop();
                        Ok(())
                    },
                    async move |_| -> Result<(), AktorError<BrokenDrop>> {
                        hook_entered.notify_one();
                        if hook_mode == "hook payload" {
                            std::panic::panic_any(BrokenDrop);
                        }
                        if hook_mode == "hook error" {
                            return Err(AktorError {
                                diagnostics: "original hook error".into(),
                                data: BrokenDrop,
                            });
                        }
                        if hook_mode == "hook driver" {
                            core::future::pending::<()>().await;
                        }
                        Ok(())
                    },
                ));

                ready.notified().await;
                if mode == "driver" {
                    task.abort();
                } else if mode == "actor driver" {
                    actor_ready.notified().await;
                    task.abort();
                    release.notify_one();
                } else if mode == "hook driver" {
                    hook_ready.notified().await;
                    task.abort();
                } else if mode == "reentrant stop" || mode == "reentrant failure" {
                    use std::{
                        future::Future,
                        task::{Context, Waker},
                    };
                    // Use a fresh group so the waiter is registered before stopping.
                    let mut fresh = AktorGroup::with_grace(Duration::from_millis(500));
                    let stopping = fresh.killswitch();
                    let fresh_done = fresh.start().unwrap();
                    let actor = fresh.spawn_value("reentrant", ()).await.unwrap();
                    let failure = mode == "reentrant failure";
                    let waker = Waker::from(Arc::new(StopWake {
                        kill: stopping.clone(),
                        actor: failure.then(|| actor.actor.new_controller()),
                    }));
                    let mut waiting = Box::pin(stopping.wait_stopping());
                    assert!(
                        waiting
                            .as_mut()
                            .poll(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                    if failure {
                        actor.actor.cancel();
                    }
                    let racers: Vec<_> = (0..2)
                        .map(|_| {
                            let stop = stopping.clone();
                            std::thread::spawn(move || stop.stop())
                        })
                        .collect();
                    for racer in racers {
                        racer.join().unwrap();
                    }
                    assert_eq!(fresh_done.wait().await.failed(), failure);
                    drop(waiting);
                }
                let result = task.await;
                if mode.ends_with("driver") {
                    assert!(result.unwrap_err().is_cancelled());
                } else {
                    assert!(result.is_ok());
                }

                let report = completed.wait().await;
                assert!(cleaned.load(Ordering::SeqCst));
                assert_eq!(report.actors.len(), 1);
                assert!(!report.timed_out);
                if mode.ends_with("driver") {
                    assert!(
                        report
                            .application
                            .iter()
                            .any(|error| error.diagnostics.contains("cancelled"))
                    );
                }
                if mode.ends_with("payload") {
                    assert_eq!(
                        report.failure.as_ref().unwrap().message,
                        "panic payload had no message"
                    );
                }
                if mode.ends_with("error") {
                    assert!(
                        report
                            .application
                            .iter()
                            .any(|error| error.diagnostics.contains("original"))
                    );
                }
            });
        return;
    }

    for mode in [
        "driver",
        "actor driver",
        "hook driver",
        "application payload",
        "hook payload",
        "application error",
        "hook error",
        "reentrant stop",
        "reentrant failure",
    ] {
        let output = support::child("shutdown_ownership", mode);
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn stop_before_start() {
    if std::env::var_os("AKTOR_CHILD").is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                for shutdown in [false, true] {
                    let mut group = AktorGroup::with_grace(Duration::from_millis(20));
                    let completion = group.completion();
                    let kill = group.killswitch();
                    if shutdown {
                        drop(group.shutdown());
                    } else {
                        kill.stop();
                    }
                    kill.stop();
                    assert!(!completion.wait().await.failed());
                    assert!(!group.shutdown().await.failed());
                    assert!(group.start().is_err());
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            });
        return;
    }

    let output = support::child("stop_before_start", "early stop");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
