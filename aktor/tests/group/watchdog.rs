use super::*;
use core::num::NonZeroUsize;

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
                let group = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::AbortProcess(()),
                    actors: AktorNew {
                        name: "broken formatter".into(),
                        kind: AktorKind::TokioThread,
                        start: aktor::AktorInit::Ready(async || ()),
                        end: |_| Err::<(), _>(((), format!("{broken}", broken = Broken))),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_millis(200),
                });
                group.ready().await.unwrap();
                let actor = group.handles();
                let _error = actor.shutdown().await.unwrap_err();
                let report = group.stop().await;

                assert!(report.to_string().contains("error display broke"));
                assert!(!report.ran_past_grace());
            });

            return;
        }

        if mode == "runtime exit" || mode == "runtime exit actor" {
            let cleaned = Arc::new(AtomicBool::new(false));
            let cleanup = cleaned.clone();
            let completed = runtime.block_on(async {
                let started = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::AbortProcess(()),
                    actors: AktorNew {
                        name: "abandoned".into(),
                        kind: AktorKind::TokioThread,
                        start: aktor::AktorInit::Ready(async move || 7_u32),
                        end: (),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: move |_| {
                        cleanup.store(true, Ordering::SeqCst);
                    },
                    grace: Duration::from_millis(200),
                });
                started.ready().await.unwrap();
                if mode == "runtime exit" {
                    started.handles().shutdown().await.unwrap();
                }
                started.completion()
            });

            drop(runtime);
            std::thread::sleep(Duration::from_millis(300));

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let report = runtime.block_on(completed.wait()).unwrap();

            assert!(cleaned.load(Ordering::SeqCst));
            assert!(!report.failed());
            assert!(!report.ran_past_grace());
            return;
        }

        if mode == "stderr failure" {
            std::panic::set_hook(Box::new(|_| {}));
            runtime.block_on(async {
                let started = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::AbortProcess(()),
                    actors: AktorNew {
                        name: "reporting".into(),
                        kind: AktorKind::TokioThread,
                        start: aktor::AktorInit::Ready(async move || 0usize),
                        end: |_| -> () { panic!("report failure") },
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_millis(200),
                });
                started.ready().await.unwrap();
                let (ready, acquired) = std::sync::mpsc::channel();

                std::thread::spawn(move || {
                    let stderr = std::io::stderr();
                    let _locked = stderr.lock();

                    ready.send(()).unwrap();
                    std::thread::sleep(Duration::from_secs(10));
                });

                acquired.recv().unwrap();

                let report = started.stop().await;

                assert!(report.failures[0].error.contains("report failure"));
                assert!(!report.ran_past_grace());
            });

            return;
        }

        if mode == "pending setup" {
            runtime.block_on(async {
                let entered = Arc::new(Notify::new());
                let inside = entered.clone();
                let dropped = Arc::new(AtomicBool::new(false));
                let cleanup = dropped.clone();
                let started = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::AbortProcess(()),
                    actors: AktorNew {
                        name: "pending setup".into(),
                        kind: AktorKind::TokioThread,
                        start: aktor::AktorInit::Ready(async move || {
                            let _drop = StartupDrop(cleanup);

                            inside.notify_one();
                            core::future::pending::<()>().await;
                        }),
                        end: async |_| (),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::new(32).unwrap(),
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace: Duration::from_secs(1),
                });
                let kill = started.killswitch();
                let closing = started.completion();
                let startup = tokio::spawn(async move { started.ready().await.is_err() });

                entered.notified().await;
                kill.stop();

                let report = closing.wait().await.unwrap();

                assert!(dropped.load(Ordering::SeqCst));
                assert!(report.ran_past_grace());
                assert!(report.failures.is_empty());
                assert!(startup.await.unwrap());
                tokio::time::sleep(Duration::from_millis(250)).await;
            });

            return;
        }

        if mode == "settling thread" {
            runtime.block_on(async {
                struct ThreadExit(Arc<AtomicBool>);
                impl Drop for ThreadExit {
                    fn drop(&mut self) {
                        std::thread::sleep(Duration::from_millis(5));
                        self.0.store(true, Ordering::SeqCst);
                    }
                }
                thread_local! {
                    static EXIT: std::cell::RefCell<Option<ThreadExit>> = const { std::cell::RefCell::new(None) };
                }

                let finished = Arc::new(AtomicBool::new(false));
                let exit = ThreadExit(finished.clone());
                let started = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::AbortProcess(()),
                    actors: AktorNew {
                        name: "settling".into(), kind: AktorKind::TokioThread,
                        start: aktor::AktorInit::Ready(async move || {
                            EXIT.set(Some(exit));

                        }),
                        end: |_| core::future::pending::<()>(),
                        hooks: AktorHooks::NONE, capacity: NonZeroUsize::MIN, stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {}, grace: Duration::from_millis(200),
                });
                started.ready().await.unwrap();

                assert!(started.stop().await.ran_past_grace());
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

                let started = aktor_start(AktorSetup {
                    stuck: aktor::AktorGroupStuck::AbortProcess(()),
                    actors: AktorNew {
                        name: "late".into(),
                        kind: AktorKind::TokioThread,
                        start: aktor::AktorInit::Ready(async || ()),
                        end: |_| std::thread::sleep(Duration::from_secs(2)),
                        hooks: AktorHooks::NONE,
                        capacity: NonZeroUsize::MIN,
                        stuck: AktorStuck::Kill,
                    },
                    shutdown: |_| {},
                    grace,
                });
                started.ready().await.unwrap();
                assert!(started.stop().await.ran_past_grace());

                core::future::pending::<()>().await;
            });

            return;
        }

        runtime.block_on(async {
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

            let (entered, entering) = tokio::sync::oneshot::channel();
            let started = aktor_start(AktorSetup {
                stuck: aktor::AktorGroupStuck::AbortProcess(()),
                actors: AktorNew {
                    name: "blocking cleanup".into(),
                    kind: AktorKind::TokioThread,
                    start: aktor::AktorInit::Ready(async move || {
                        if setup_blocks {
                            entered.send(()).unwrap();
                            loop {
                                std::thread::sleep(Duration::from_secs(10));
                            }
                        }
                    }),
                    end: move |_| {
                        if actor_blocks {
                            loop {
                                std::thread::sleep(Duration::from_secs(10));
                            }
                        }
                    },
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::MIN,
                    stuck: AktorStuck::Kill,
                },
                shutdown: async move |_| {
                    if !actor_blocks && !setup_blocks {
                        loop {
                            std::thread::sleep(Duration::from_secs(10));
                        }
                    }
                },
                grace: Duration::from_millis(200),
            });
            if setup_blocks {
                entering.await.unwrap();
            } else {
                started.ready().await.unwrap();
            }
            let _report = started.stop().await;

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

    let (entered, inside) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: support::thread_actor((), 1),
        shutdown: async move |_| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Err::<(), _>(((), String::from("late close failure")))
        },
        grace: Duration::from_millis(200),
    });
    started.ready().await.unwrap();
    let closing = started.completion();
    started.killswitch().stop();
    inside.await.unwrap();

    let (cleaned, cleaning) = tokio::sync::oneshot::channel();
    let sibling = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: support::thread_actor((), 1),
        shutdown: move |_| {
            let _ = cleaned.send(());
        },
        grace: Duration::from_secs(1),
    });
    sibling.ready().await.unwrap();
    sibling.killswitch().stop();
    tokio::time::timeout(Duration::from_secs(2), cleaning)
        .await
        .unwrap()
        .unwrap();

    let report = closing.wait().await.unwrap();
    assert!(report.ran_past_grace());

    release.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let report = closing.try_report().unwrap();

            if report
                .failures
                .iter()
                .any(|failure| failure.error.contains("late close failure"))
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("late callback error was lost");
}
