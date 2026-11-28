use super::*;
use core::num::NonZeroUsize;

#[cfg(feature = "testing")]
#[tokio::test]
async fn value() {
    let actors = AktorGroup::new();

    let early = AktorNew {
        name: "early".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Ready(async || 0usize),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    };

    assert!(actors.spawn(early).ready().await.is_err());

    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "counter".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async move || 7usize),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| Err::<(), _>(((), String::from("application cleanup error"))),
        grace: Duration::from_secs(1),
    });
    started.ready().await.unwrap();
    let kill = started.killswitch();
    let completed = started.completion();
    let closing = started.completion();

    let waiter = tokio::spawn(async move { closing.wait().await });

    tokio::task::yield_now().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());

    assert_eq!(call(started.handles(), async |state| *state).await, Ok(7));
    kill.stop();

    let report = tokio::time::timeout(Duration::from_secs(2), completed.wait())
        .await
        .unwrap()
        .unwrap();

    assert!(
        report.failures[0]
            .error
            .contains("application cleanup error")
    );
}

#[tokio::test]
async fn drop_group() {
    for return_error in [false, true] {
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleanup = cleaned.clone();
        let started = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "owned actor".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || ()),
                end: move |_| {
                    cleanup.store(true, Ordering::SeqCst);
                },
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(1),
        });
        started.ready().await.unwrap();
        let kill = started.killswitch();
        let closing = started.completion();
        let (actors, actor) = started.into_parts();

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
            .unwrap()
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
                    let group = AktorGroup::with_grace(Duration::from_millis(20));
                    let completion = group.completion();
                    let kill = group.killswitch();

                    if shutdown {
                        drop(group.stop());
                    } else {
                        kill.stop();
                    }

                    kill.stop();
                    assert!(!completion.wait().await.unwrap().failed());
                    assert!(!group.stop().await.failed());
                    assert!(
                        group
                            .spawn(support::thread_actor((), 1))
                            .ready()
                            .await
                            .is_err()
                    );
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
