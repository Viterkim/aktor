use super::*;
use aktor::AktorGroupShutdownError;

struct StartupDrop(Arc<AtomicBool>);

impl Drop for StartupDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn shutdown() {
    if let Ok(mode) = std::env::var("AKTOR_GROUP_BLOCKING_PROOF") {
        eprintln!("watchdog case: {mode}");
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

        if mode == "formatter panic" {
            struct Broken;
            impl std::fmt::Display for Broken {
                fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    panic!("error display broke");
                }
            }
            impl std::fmt::Debug for Broken {
                fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    panic!("error debug broke");
                }
            }
            impl std::error::Error for Broken {}

            runtime.block_on(async {
                let mut group = AktorGroup::with_policy(
                    Duration::from_millis(200),
                    AktorShutdownPolicy::AbortProcess,
                );

                group.start().unwrap();
                let actor = group
                    .spawn_args(AktorArgs::new(
                        "broken formatter",
                        || Ok::<_, String>(()),
                        |_| Err::<(), _>(format!("{broken}", broken = Broken)),
                    ))
                    .await
                    .unwrap();
                let _error = actor.shutdown().await.unwrap_err();
                let report = group.shutdown().await;

                assert!(report.to_string().contains("error display broke"));
                assert!(!report.ran_past_grace);
            });

            return;
        }

        if mode == "runtime exit" || mode == "runtime exit actor" {
            let cleaned = Arc::new(AtomicBool::new(false));
            let cleanup = cleaned.clone();
            let completed = runtime.block_on(async {
                let mut group = AktorGroup::with_policy(
                    Duration::from_millis(200),
                    AktorShutdownPolicy::AbortProcess,
                );
                let completed = group
                    .start_with(async move |_| {
                        cleanup.store(true, Ordering::SeqCst);
                        Ok::<_, String>(())
                    })
                    .unwrap();

                if mode == "runtime exit actor" {
                    group.spawn_value("abandoned", 7_u32).await.unwrap();
                }

                completed
            });

            drop(runtime);
            std::thread::sleep(Duration::from_millis(300));

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let report = runtime.block_on(completed.wait());

            assert!(cleaned.load(Ordering::SeqCst));
            assert!(!report.failed());
            assert!(!report.ran_past_grace);
            return;
        }

        if mode == "stderr failure" {
            std::panic::set_hook(Box::new(|_| {}));
            runtime.block_on(async {
                let mut group = AktorGroup::with_policy(
                    Duration::from_millis(200),
                    AktorShutdownPolicy::AbortProcess,
                );
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
                assert!(!report.ran_past_grace);
            });

            return;
        }

        if mode == "pending setup" {
            runtime.block_on(async {
                let mut group = AktorGroup::with_policy(
                    Duration::from_secs(1),
                    AktorShutdownPolicy::AbortProcess,
                );
                let kill = group.killswitch();
                let closing = group.start_with(async |_| Ok::<_, String>(())).unwrap();
                let entered = Arc::new(Notify::new());
                let inside = entered.clone();
                let dropped = Arc::new(AtomicBool::new(false));
                let cleanup = dropped.clone();
                let startup = tokio::spawn(async move {
                    group
                        .spawn_async(AktorArgs::new(
                            "pending setup",
                            async move || {
                                let _drop = StartupDrop(cleanup);

                                inside.notify_one();
                                core::future::pending::<()>().await;
                                Ok::<_, String>(())
                            },
                            async |_| Ok::<_, String>(()),
                        ))
                        .await
                });

                entered.notified().await;
                kill.stop();

                let report = closing.wait().await;

                assert!(dropped.load(Ordering::SeqCst));
                assert!(report.ran_past_grace);
                assert!(report.failure.is_none());
                let result = startup.await.unwrap().map(|_| ());
                assert!(
                    matches!(
                        &result,
                        Err((error, _)) if matches!(error.cause.primary(),
                            aktor::listener::DedicatedStartError::Closed)
                    ),
                    "{result:?}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            });

            return;
        }

        if mode == "settling thread" {
            runtime.block_on(async {
                let mut group = AktorGroup::with_policy(
                    Duration::from_secs(1),
                    AktorShutdownPolicy::AbortProcess,
                );
                let closing = group.start().unwrap();
                let lifetime = group
                    .killswitch()
                    .track_thread("settling".into(), aktor::AktorStuck::Kill);
                let cancelled = Arc::new(Notify::new());
                let cancel = cancelled.clone();
                let (release, released) = tokio::sync::oneshot::channel();
                let finished = Arc::new(AtomicBool::new(false));
                let thread_finished = finished.clone();

                std::thread::spawn(move || {
                    released.blocking_recv().unwrap();
                    std::thread::sleep(Duration::from_millis(5));
                    thread_finished.store(true, Ordering::SeqCst);
                    drop(lifetime);
                });

                group
                    .register_owner(
                        "settling".into(),
                        aktor::AktorExecution::TokioThread,
                        Box::new(|| {}),
                        Box::new(move || cancel.notify_one()),
                        None,
                        None,
                        Box::pin(async move {
                            cancelled.notified().await;
                            release.send(()).unwrap();
                            aktor::AktorOutcome {
                                retirement: aktor::AktorRetireStatus::Unconfirmed,
                                actor: "settling".into(),
                                kind: aktor::AktorExecution::TokioThread,
                                diagnostics: vec![],
                                ran_past_grace: true,
                            }
                        }),
                    )
                    .unwrap();

                group.killswitch().stop();
                assert!(closing.wait().await.ran_past_grace);
                assert!(finished.load(Ordering::SeqCst), "thread was still settling");
                tokio::time::sleep(Duration::from_millis(150)).await;
            });

            return;
        }

        if matches!(&*mode, "late completion" | "zero grace" | "tiny grace") {
            runtime.block_on(async {
                let grace = match &*mode {
                    "zero grace" => Duration::ZERO,
                    "tiny grace" => Duration::from_millis(1),
                    _ => Duration::from_millis(300),
                };

                let mut group = AktorGroup::with_policy(grace, AktorShutdownPolicy::AbortProcess);
                let closing = group.start().unwrap();
                let lifetime = group
                    .killswitch()
                    .track_thread("late".into(), aktor::AktorStuck::Kill);

                group
                    .register_owner(
                        "late".into(),
                        aktor::AktorExecution::StdThread,
                        Box::new(|| {}),
                        Box::new(|| {}),
                        None,
                        None,
                        Box::pin(core::future::pending()),
                    )
                    .unwrap();

                group.killswitch().stop();
                assert!(closing.await.ran_past_grace);

                // Finishing after the deadline doesn't save the process.
                std::thread::sleep(Duration::from_secs(2));
                drop(lifetime);
                core::future::pending::<()>().await;
            });

            return;
        }

        runtime.block_on(async {
            let mut group = AktorGroup::with_policy(
                Duration::from_millis(200),
                AktorShutdownPolicy::AbortProcess,
            );
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

            let completed = group
                .start_with(async move |_| {
                    if !actor_blocks && !setup_blocks {
                        loop {
                            std::thread::sleep(Duration::from_secs(10));
                        }
                    }

                    Ok::<_, String>(())
                })
                .unwrap();
            let killswitch = group.killswitch();
            let app = &mut group;

            killswitch
                .run(async {
                    let _actor = app
                        .spawn_args(AktorArgs {
                            name: "blocking cleanup".into(),
                            capacity: 1,
                            setup: move || {
                                if setup_blocks {
                                    loop {
                                        std::thread::sleep(Duration::from_secs(10));
                                    }
                                }

                                Ok::<_, String>(())
                            },
                            cleanup: move |_| {
                                if actor_blocks {
                                    loop {
                                        std::thread::sleep(Duration::from_secs(10));
                                    }
                                }

                                Ok::<_, String>(())
                            },
                        })
                        .await
                        .unwrap();

                    kill.stop();
                    core::future::pending::<()>().await;
                })
                .await;
            killswitch.stop();

            let _report = completed.wait().await;

            // An uninterruptible actor must still cause exit if the report was ignored.
            std::thread::sleep(Duration::from_secs(10));
        });

        return;
    }

    for mode in [
        "runtime exit",
        "runtime exit actor",
        "formatter panic",
        "actor",
        "application",
        "setup",
        "pending setup",
        "settling thread",
        "late completion",
        "zero grace",
        "tiny grace",
        "stderr",
        "stderr failure",
        "exit handler",
        #[cfg(unix)]
        "abort handler",
    ] {
        let mut process = support::test_command("watchdog::shutdown")
            .env("AKTOR_GROUP_BLOCKING_PROOF", mode)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(3);

        loop {
            if process.try_wait().unwrap().is_some() {
                break;
            }

            if std::time::Instant::now() >= deadline {
                process.kill().unwrap();
                process.wait().unwrap();
                panic!("shutdown watchdog did not stop blocking {mode}");
            }

            std::thread::sleep(Duration::from_millis(10));
        }

        let output = process.wait_with_output().unwrap();
        let forced_exit = !matches!(
            mode,
            "runtime exit"
                | "runtime exit actor"
                | "formatter panic"
                | "pending setup"
                | "settling thread"
                | "stderr failure"
        );

        assert!(
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .any(|line| line == format!("watchdog case: {mode}")),
            "watchdog process did not enter {mode}"
        );

        if forced_exit {
            assert!(!output.status.success());
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(
                    output.status.signal(),
                    Some(9),
                    "{mode}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        } else {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        if !forced_exit {
            assert!(
                !String::from_utf8_lossy(&output.stderr).contains("Shutdown ran past its grace")
            );
        }
    }
}

#[tokio::test]
async fn default_callback() {
    if std::env::var_os("AKTOR_TEST_CASE").is_none() {
        let output = support::run_test("watchdog::default_callback", "blocked callback");

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );

        return;
    }

    let mut group = AktorGroup::with_grace(Duration::from_millis(200));
    let (entered, inside) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let closing = group
        .start_with(async move |_| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Err::<(), _>(String::from("late close failure"))
        })
        .unwrap();

    group.killswitch().stop();
    inside.await.unwrap();

    let report = closing.wait().await;
    let timeout = Some(AktorGroupShutdownError::CallbackTimedOut);

    assert_eq!(report.group_error, timeout);
    assert!(report.ran_past_grace);

    release.send(()).unwrap();

    let report = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let report = closing.try_report().unwrap();

            if report
                .application
                .iter()
                .any(|error| error.diagnostics == "late close failure")
            {
                return report;
            }

            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("late callback error was lost");

    assert_eq!(report.group_error, timeout);
}
