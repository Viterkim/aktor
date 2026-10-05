use super::*;
use std::time::Duration;
use tokio::sync::Notify;

#[aktor]
async fn pending(state: &mut u32, entered: Arc<Notify>) {
    *state = 85;
    entered.notify_one();
    core::future::pending::<()>().await;
}

#[tokio::test]
async fn cancellation() {
    if std::env::var("AKTOR_CHILD").is_err() {
        let output = support::child("task::cancellation", "task");

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let cleaned = Arc::new(Mutex::new(None));
    let saved = cleaned.clone();
    let actors = start(AktorSetup {
        name: AktorName::new("cancelled task"),
        role: AktorNoRole,
        kind: AktorKind::TokioTask,
        closures: AktorClosures {
            start: async || Ok(0_u32),
            end: Some(
                (async move |state: u32| {
                    *saved.lock().unwrap() = Some(state);
                    Ok(())
                })
                .into(),
            ),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: Some(AktorOptions {
            shutdown_grace: Duration::from_millis(400),
            ..Default::default()
        }),
    })
    .await
    .unwrap();

    let entered = Arc::new(Notify::new());

    drop(pending(&actors.handles, entered.clone()).send().await);
    entered.notified().await;

    let report = actors.shutdown().await;

    assert!(report.timed_out);
    assert_eq!(cleaned.lock().unwrap().take(), Some(85));
    assert_eq!(
        actors.completion().wait().await.actors[0].kind,
        Some(AktorExecution::TokioTask)
    );
}
