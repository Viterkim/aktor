use super::*;
use aktor::setup::AktorStart;
use std::time::Duration;

async fn cleanup_panics(_: ()) -> Result<(), String> {
    panic!("cleanup interrupted");
}

async fn start<Setup>(setup: Setup, grace: Duration) -> AktorStarted<Setup::Handles, AktorGroup>
where
    Setup: AktorStart<Group = AktorGroup>,
{
    aktor_start(AktorSetup {
        actors: setup,
        shutdown: |_| {},
        grace,
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn cleanup_receipt() {
    macro_rules! actor {
        ($kind:expr, $end:expr) => {
            AktorNew {
                name: "cleanup receipt".into(),
                kind: $kind,
                start: async || aktor_ready(()),
                end: $end,
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            }
        };
    }

    let actors = start(
        actor!(AktorKind::TokioThread, cleanup_panics),
        Duration::from_secs(5),
    )
    .await;
    let completion = actors.handles.completion();
    assert_eq!(
        completion.cleanup_status(),
        aktor::AktorCleanupStatus::Pending
    );
    let report = actors.group.shutdown().wait_retired().await.unwrap();

    assert_eq!(report.actors[0].retirement, AktorRetireStatus::Terminated);
    assert_eq!(
        completion.cleanup_status(),
        aktor::AktorCleanupStatus::Unconfirmed
    );
    assert_eq!(
        completion.wait_cleanup().await.unwrap_err().0,
        owner::CleanupError::Unconfirmed
    );

    let actors = start(
        actor!(AktorKind::TokioThread, |_| std::future::pending::<()>()),
        Duration::from_millis(20),
    )
    .await;
    let completion = actors.handles.completion();
    let report = actors.group.shutdown().wait_retired().await.unwrap();

    assert!(report.ran_past_grace);
    assert_eq!(
        completion.wait_cleanup().await.unwrap_err().0,
        owner::CleanupError::Unconfirmed
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
                let report = actors.group.shutdown().wait_retired().await.unwrap();
                assert_eq!(report.actors[0].retirement, AktorRetireStatus::Terminated);

                let actors = start(
                    actor!(
                        AktorKind::TokioLocal(&local),
                        |_| std::future::pending::<()>()
                    ),
                    Duration::from_millis(50),
                )
                .await;
                let completion = actors.handles.completion();

                actors.group.shutdown().wait_retired().await.unwrap();
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
                    start: async || aktor_ready(()),
                    end: |_| std::future::pending::<()>(),
                    hooks: Default::default(),
                    capacity: 1,
                    stuck: AktorStuck::Kill,
                },
                Duration::from_millis(100),
            )
            .await;

            let report = actors.group.shutdown().wait_retired().await.unwrap();
            assert_eq!(report.actors[0].retirement, AktorRetireStatus::Terminated);
        }));
    });
}

#[tokio::test]
async fn shutdown_receipt() {
    for force in [false, true] {
        let mut group = AktorGroup::with_grace(Duration::from_millis(300));

        group.start().unwrap();

        let actor = group
            .spawn_async(AktorArgs::new(
                "forced cleanup",
                async || Ok::<_, String>(()),
                async |_| Err::<(), _>(String::from("cleanup finished with an error")),
            ))
            .await
            .unwrap();
        let (started, running) = tokio::sync::oneshot::channel();

        message::call_async(
            &actor.handle,
            async move |_, ()| {
                started.send(()).unwrap();
                if force {
                    core::future::pending::<()>().await;
                } else {
                    panic!("operation failed before cleanup");
                }
            },
            (),
        )
        .cast()
        .await;
        running.await.unwrap();

        let report = group.shutdown().await;

        assert_eq!(report.actors[0].retirement, AktorRetireStatus::Confirmed);
        let (error, detail) = actor.completion().wait_cleanup().await.unwrap_err();
        assert_eq!(
            (error, detail.as_str()),
            (
                owner::CleanupError::Failed,
                "cleanup finished with an error"
            )
        );
        assert_eq!(
            actor.control.shutdown().await.unwrap_err().0,
            owner::CleanupError::Failed
        );
    }
}

#[tokio::test]
async fn retire_with_reply() {
    for draining in [false, true] {
        let actors = start(
            AktorNew {
                name: "last reply".into(),
                kind: AktorKind::TokioThread,
                start: async || aktor_ready(()),
                end: (),
                hooks: Default::default(),
                capacity: 3,
                stuck: AktorStuck::Kill,
            },
            Duration::from_secs(5),
        )
        .await;
        let actor = actors.handles;
        let (release, held) = tokio::sync::oneshot::channel();

        message::call_async(&actor.handle, async move |_, ()| held.await.unwrap(), ())
            .cast()
            .await;
        let owner = actor.clone();
        let last = message::call(
            &actor.handle,
            move |_, ()| {
                owner.retire();
                Err::<(), u8>(85)
            },
            (),
        )
        .admit()
        .await
        .unwrap();
        let queued = message::call(&actor.handle, |_, ()| (), ())
            .admit()
            .await
            .unwrap();

        if draining {
            actor.shutdown();
        }
        release.send(()).unwrap();

        assert_eq!(last.receive_settled().await.unwrap(), Err(85));
        assert_eq!(
            queued.receive_settled().await,
            Err(message::CallError::Discarded)
        );
        assert!(matches!(
            message::call(&actor.handle, |_, ()| (), ()).admit().await,
            Err(message::CallError::NotAdmitted)
        ));
        actor.completion().wait_cleanup().await.unwrap();
        actors.group.completion().await.into_result().unwrap();
    }
}
