use super::*;

struct BrokenDrop;

impl Drop for BrokenDrop {
    fn drop(&mut self) {
        panic!("application destructor failed");
    }
}

#[tokio::test]
async fn cancellation() {
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

#[tokio::test]
async fn value() {
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
async fn drop_group() {
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
fn shutdown() {
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
        let output = support::child("ownership::shutdown", mode);

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

    let output = support::child("ownership::stop_before_start", "early stop");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[should_panic(expected = "unknown child test: missing_child_test")]
fn missing_child() {
    support::child("missing_child_test", "invalid");
}
