use super::*;

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

        if mode == "settling thread" {
            runtime.block_on(async {
                let mut group = AktorGroup::with_grace(Duration::from_secs(1));
                let closing = group.start().unwrap();
                let lifetime = group.killswitch().track_thread("settling".into());
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
                        Box::pin(async move {
                            cancelled.notified().await;
                            release.send(()).unwrap();
                            aktor::ActorOutcome {
                                actor: "settling".into(),
                                kind: None,
                                diagnostics: vec![],
                                timed_out: true,
                            }
                        }),
                    )
                    .unwrap();

                group.killswitch().stop();
                assert!(closing.wait().await.timed_out);
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

                let mut group = AktorGroup::with_grace(grace);
                let closing = group.start().unwrap();
                let lifetime = group.killswitch().track_thread("late".into());

                group
                    .register_owner(
                        "late".into(),
                        aktor::AktorExecution::StdThread,
                        Box::new(|| {}),
                        Box::new(|| {}),
                        Box::pin(core::future::pending()),
                    )
                    .unwrap();

                group.killswitch().stop();
                assert!(closing.await.timed_out);
                drop(lifetime);
                core::future::pending::<()>().await;
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
        let mut child = support::child_command("watchdog::shutdown")
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
        let forced_exit = !matches!(mode, "pending setup" | "settling thread" | "stderr failure");

        assert!(
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .any(|line| line == format!("watchdog case: {mode}")),
            "watchdog child did not enter {mode}"
        );

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
