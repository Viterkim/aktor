use super::*;
use core::num::NonZeroUsize;
use std::time::Duration;

async fn cleanup_panics(_: ()) {
    panic!("cleanup interrupted");
}

async fn start<Setup>(
    setup: Setup,
    grace: Duration,
) -> AktorStarted<Setup::Handles, AktorGroup, Setup::Error>
where
    Setup: AktorStart<Group = AktorGroup>,
    Setup::Error: std::fmt::Debug,
{
    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: setup,
        shutdown: |_| {},
        grace,
    });

    started.ready().await.unwrap();
    started
}

#[tokio::test]
async fn clean_release_past_grace() {
    let (release, released) = tokio::sync::oneshot::channel();
    let actors = start(
        AktorNew {
            name: "checkpoint".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || ()),
            end: async move |()| released.await.unwrap(),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Leave,
        },
        Duration::from_millis(50),
    )
    .await;

    let stopped = actors.stop().await;
    assert!(!stopped.released());

    release.send(()).unwrap();
    let report = actors.stop_released().await.unwrap().into_report();
    assert!(report.ran_past_grace());
    assert!(!report.failed(), "{report}");
}

#[tokio::test]
async fn cleanup_receipt() {
    macro_rules! actor {
        ($kind:expr, $end:expr) => {
            AktorNew {
                name: "cleanup receipt".into(),
                kind: $kind,
                start: aktor::AktorInit::Ready(async || ()),
                end: $end,
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            }
        };
    }

    let actors = start(
        actor!(AktorKind::TokioThread, cleanup_panics),
        Duration::from_secs(5),
    )
    .await;
    let completion = actors.handles().completion();
    let report = actors.stop_released().await.unwrap().into_report();

    assert_eq!(
        report.actors[0].retirement(),
        Some(AktorRetirement::Terminated)
    );
    assert_eq!(
        completion.wait_cleanup().await.unwrap_err().0,
        owner::CleanupError::Unconfirmed(Some(
            report
                .actor_failures(report.actors[0].id)
                .next()
                .unwrap()
                .id
        ))
    );

    let actors = start(
        actor!(AktorKind::TokioThread, |_| std::future::pending::<()>()),
        Duration::from_millis(20),
    )
    .await;
    let completion = actors.handles().completion();
    let report = actors.stop_released().await.unwrap().into_report();

    assert!(report.ran_past_grace());
    assert_eq!(
        completion.wait_cleanup().await.unwrap_err().0,
        owner::CleanupError::Unconfirmed(Some(
            report
                .actor_failures(report.actors[0].id)
                .next()
                .unwrap()
                .id
        ))
    );
    assert!(matches!(
        &*completion.wait().await.unwrap_err().0,
        owner::OwnerError::CleanupUnconfirmed
    ));

    #[cfg(feature = "local")]
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let actors = start(
                    actor!(AktorKind::TokioLocal(&local), cleanup_panics),
                    Duration::from_secs(5),
                )
                .await;
                let report = actors.stop_released().await.unwrap().into_report();
                assert_eq!(
                    report.actors[0].retirement(),
                    Some(AktorRetirement::Terminated)
                );

                let actors = start(
                    actor!(
                        AktorKind::TokioLocal(&local),
                        |_| std::future::pending::<()>()
                    ),
                    Duration::from_millis(50),
                )
                .await;
                let completion = actors.handles().completion();

                let _ = actors.stop_released().await.unwrap().into_report();
                assert!(matches!(
                    completion.wait().await.unwrap_err().0,
                    aktor::local::OwnerError::CleanupUnconfirmed
                ));
            })
            .await;
    }
}

#[cfg(feature = "bevy")]
#[test]
fn bevy_local_cleanup_receipt() {
    let pool = bevy_tasks::TaskPoolBuilder::new().num_threads(1).build();

    pool.with_local_executor(|executor| {
        aktor::executor::block_on(executor.run(async {
            let actors = start(
                AktorNew {
                    name: "Bevy local cleanup".into(),
                    kind: AktorKind::BevyLocal(&pool),
                    start: aktor::AktorInit::Ready(async || ()),
                    end: |_| std::future::pending::<()>(),
                    hooks: AktorHooks::NONE,
                    capacity: NonZeroUsize::MIN,
                    stuck: AktorStuck::Kill,
                },
                Duration::from_millis(100),
            )
            .await;

            let report = actors.stop_released().await.unwrap().into_report();
            assert_eq!(
                report.actors[0].retirement(),
                Some(AktorRetirement::Terminated)
            );
        }));
    });
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn shutdown_receipt() {
    for force in [false, true] {
        let actors = start(
            AktorNew {
                name: "forced cleanup".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || ()),
                end: async |_| Err::<(), _>(((), String::from("cleanup finished with an error"))),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            Duration::from_millis(300),
        )
        .await;
        let actor = actors.handles();
        let (started, running) = tokio::sync::oneshot::channel();

        aktor::testing::call(actor, async move |_| {
            started.send(()).unwrap();
            if force {
                core::future::pending::<()>().await;
            } else {
                panic!("operation failed before cleanup");
            }
        })
        .cast()
        .await
        .unwrap();
        running.await.unwrap();

        let report = actors.stop().await;

        assert_eq!(
            report.actors[0].retirement(),
            Some(AktorRetirement::Released)
        );
        assert!(
            report
                .to_string()
                .contains("cleanup finished with an error")
        );
        let owner::CleanupError::Failed(id) =
            actor.completion().wait_cleanup().await.unwrap_err().0
        else {
            panic!("end returned its failure");
        };
        assert!(
            report
                .failure(id)
                .unwrap()
                .error
                .contains("cleanup finished with an error")
        );
    }
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn retire_with_reply() {
    for draining in [false, true] {
        let actors = start(
            AktorNew {
                name: "last reply".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || ()),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(3).unwrap(),
                stuck: AktorStuck::Kill,
            },
            Duration::from_secs(5),
        )
        .await;
        let actor = actors.handles();
        let (release, held) = tokio::sync::oneshot::channel();

        aktor::testing::call(actor, async move |_| held.await.unwrap())
            .cast()
            .await
            .unwrap();
        let owner = actor.clone();
        let last = aktor::testing::call(actor, async move |_| {
            owner.retire();
            Err::<(), u8>(85)
        })
        .send()
        .await
        .unwrap();
        let queued = aktor::testing::call(actor, async |_| ())
            .send()
            .await
            .unwrap();

        if draining {
            actor.shutdown();
        }
        release.send(()).unwrap();

        assert_eq!(last.await.unwrap(), Err(85));
        assert!(matches!(
            queued.await,
            Err(aktor::AktorDeliveryError {
                outcome: message::AktorDeliveryOutcome::Discarded,
                ..
            })
        ));
        assert!(matches!(
            aktor::testing::call(actor, async |_| ()).send().await,
            Err(aktor::AktorDeliveryError {
                outcome: message::AktorDeliveryOutcome::NotAdmitted,
                ..
            })
        ));
        actor.completion().wait_cleanup().await.unwrap();
        actors
            .group()
            .completion()
            .await
            .unwrap()
            .into_result()
            .unwrap();
    }
}
