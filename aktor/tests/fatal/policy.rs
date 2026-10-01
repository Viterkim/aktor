use super::*;

struct State;
impl Drop for State {
    fn drop(&mut self) {
        eprintln!("state cleaned");
    }
}

#[tokio::test]
async fn abort() {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            if let Ok(case) = std::env::var("AKTOR_CHILD") {
                match case.as_str() {
                    "dedicated" => {
                        let (handle, _, thread) = spawn(SpawnArgs {
                            name: "fatal".into(),
                            capacity: 1,
                            failure: FailurePolicy::Abort,
                            setup: || Ok::<_, std::convert::Infallible>(0usize),
                            cleanup: |_| {
                                eprintln!("state cleaned");
                                Ok::<_, std::convert::Infallible>(())
                            },
                        })
                        .await
                        .unwrap();

                        crash(&handle).cast().await;
                        let _result = thread.join_async().await;
                    }

                    "hook" | "payload" => {
                        let (handle, mut listener) = channel::<usize>(1).unwrap();
                        listener.failure = FailurePolicy::shutdown(move |_| {
                            if case == "payload" {
                                std::panic::panic_any(Bomb);
                            }

                            panic!("shutdown hook failed");
                        });

                        let task = tokio::task::spawn_local(listener.run(0));
                        crash(&handle).cast().await;
                        let _result = task.await;
                        eprintln!("state cleaned");
                    }

                    "task" | "cancel" | "unpolled" => {
                        let (handle, task) =
                            spawn_local_with_policy(&executor, State, 1, FailurePolicy::Abort)
                                .unwrap();

                        if case == "task" {
                            call(&handle, |_, ()| panic!("cast failed"), ())
                                .cast()
                                .await;
                        } else {
                            if case == "cancel" {
                                call(&handle, |_, ()| (), ()).await;
                            }

                            task.abort();
                        }

                        let _result = task.await;
                    }
                    _ => panic!("unknown child case"),
                }

                eprintln!("FORBIDDEN_FALLBACK");
                panic!("fatal failure did not stop the process");
            }

            for case in ["dedicated", "task", "cancel", "unpolled", "hook", "payload"] {
                let output = support::child("policy::abort", case);
                let stderr = String::from_utf8_lossy(&output.stderr);

                assert!(!stderr.contains("FORBIDDEN_FALLBACK"), "{case}: {stderr}");
                if case != "hook" && case != "payload" {
                    assert!(stderr.contains("state cleaned"), "{case}: {stderr}");
                }

                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    assert_eq!(output.status.signal(), Some(6), "{case}: {stderr}");
                }

                assert!(stderr.contains("actor "), "{case}: {stderr}");
                if case == "cancel" || case == "unpolled" {
                    assert!(stderr.contains("Cancelled"), "{stderr}");
                }

                assert!(!output.status.success(), "{case}: {stderr}");
            }
        })
        .await
}

struct Bomb;
impl Drop for Bomb {
    fn drop(&mut self) {
        panic!("queued input destructor");
    }
}

#[tokio::test]
async fn discard() {
    if let Ok(case) = std::env::var("AKTOR_CHILD") {
        let failure = if case == "hook" {
            FailurePolicy::shutdown(|_| eprintln!("HOOK_RAN"))
        } else {
            FailurePolicy::Abort
        };

        let (entered, running) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let (handle, _actor, thread) = spawn(SpawnArgs {
            name: "teardown".into(),
            capacity: 1,
            failure,
            setup: || Ok::<_, std::convert::Infallible>(()),
            cleanup: |_| Ok::<_, std::convert::Infallible>(()),
        })
        .await
        .unwrap();

        let _first = call(
            &handle,
            move |_, ()| {
                entered.send(()).unwrap();
                released.recv().unwrap();
                panic!("first failure");
            },
            (),
        )
        .checked_send()
        .await
        .unwrap();
        running.await.unwrap();

        call(&handle, |_, input| drop(input), Bomb).cast().await;
        release.send(()).unwrap();

        let _result = thread.join_async().await;
        if case != "hook" {
            eprintln!("FORBIDDEN_FALLBACK");
        }

        return;
    }

    for case in ["abort", "hook"] {
        let output = support::child("policy::discard", case);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if case == "hook" {
            assert!(stderr.contains("HOOK_RAN"), "{stderr}");
        } else {
            assert!(!stderr.contains("FORBIDDEN_FALLBACK"), "{stderr}");
            assert!(!output.status.success());
        }
    }
}
