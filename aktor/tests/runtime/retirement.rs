use super::*;
use aktor::setup::AktorStart;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

async fn cleanup_panics(_: ()) -> Result<(), AktorCleanupError> {
    panic!("cleanup interrupted");
}

async fn check<Setup, Mode>(setup: Setup)
where
    Setup: AktorStart<Mode, Group = AktorGroup>,
{
    let actors = aktor_start(AktorSetup {
        actors: setup,
        shutdown: |_| {},
        options: Default::default(),
    })
    .await
    .unwrap();
    let report = actors.shutdown().await;

    assert_eq!(report.actors[0].retirement, RetirementStatus::Unconfirmed);
}

#[tokio::test]
async fn cleanup_receipt() {
    macro_rules! actor {
        ($kind:expr) => {
            AktorNew {
                name: AktorName::new("cleanup receipt"),
                role: AktorNoRole,
                kind: $kind,
                closures: AktorClosures {
                    start: async || Ok::<_, AktorSetupError>(()),
                    end: cleanup_panics,
                    intervals: vec![],
                    before_each: None,
                    after_each: None,
                },
                options: Default::default(),
            }
        };
    }

    check(actor!(AktorKind::TokioThread)).await;

    #[cfg(feature = "local")]
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(check(actor!(AktorKind::TokioLocal(&local))))
            .await;
    }
}

#[tokio::test]
async fn shutdown_receipt() {
    for force in [false, true] {
        let entered = Arc::new(AtomicBool::new(false));
        let cleanup_entered = entered.clone();
        let mut group = AktorGroup::with_grace(Duration::from_millis(300));

        group.start().unwrap();

        let actor = group
            .spawn_async(AktorArgs::new(
                "forced cleanup",
                async || Ok::<_, AktorSetupError>(()),
                move |_| {
                    cleanup_entered.store(true, Ordering::SeqCst);
                    async {
                        Err::<(), _>(AktorCleanupError::new("cleanup finished with an error", ()))
                    }
                },
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

        assert_eq!(entered.load(Ordering::SeqCst), !force);
        assert_eq!(
            report.actors[0].retirement,
            if force {
                RetirementStatus::Unconfirmed
            } else {
                RetirementStatus::Confirmed
            }
        );
    }
}
