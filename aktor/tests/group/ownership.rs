use super::*;

#[tokio::test]
async fn value() {
    let mut actors = AktorGroup::new();

    assert!(matches!(
        actors.spawn_value("early", 0usize).await,
        Err((
            aktor::owner::StartError {
                kind: AktorStartKind::Invalid(AktorStartInvalid::GroupNotStarted),
                ..
            },
            _
        ))
    ));

    let kill = actors.killswitch();
    let completed = actors.completion();
    let closing = actors
        .start_with(async |_| Err::<(), _>(String::from("application cleanup error")))
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
        let closing = actors.start_with(async |_| Ok::<_, String>(())).unwrap();
        let actor = actors
            .spawn_args(AktorArgs::new(
                "owned actor",
                || Ok::<_, String>(()),
                move |_| {
                    cleanup.store(true, Ordering::SeqCst);
                    Ok::<_, String>(())
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

#[test]
fn stop_before_start() {
    if std::env::var_os("AKTOR_TEST_CASE").is_some() {
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

    let output = support::run_test("ownership::stop_before_start", "early stop");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn run_stops() {
    let actors = AktorGroup::new();
    let kill = actors.killswitch();

    kill.stop();

    assert_eq!(kill.run(std::future::pending::<()>()).await, None);
}
