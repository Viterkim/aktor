use crate::{
    AktorGroup, AktorHooks, AktorKind, AktorNew, AktorStartReason, AktorStuck, executor::FAIL_SPAWN,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        FAIL_SPAWN.with(|setting| setting.set(None));
    }
}

#[tokio::test]
async fn observer_failure() {
    let _reset = Reset;

    FAIL_SPAWN.with(|setting| setting.set(Some("aktor observer")));

    let started = crate::aktor_start(crate::AktorSetup {
        stuck: crate::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "cannot observe".into(),
            kind: AktorKind::TokioThread,
            start: crate::AktorInit::Ready(async || ()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: core::time::Duration::from_secs(1),
    });
    let completion = started.completion();
    let (error, _) = started.ready().await.err().unwrap();

    assert_eq!(error.reason(), AktorStartReason::InvalidSetup);
    let _ = completion.wait_retired().await.unwrap();
}

#[tokio::test]
async fn thread_failure() {
    let mut group = AktorGroup::new();

    group.start().unwrap();

    let cleaned = Arc::new(AtomicUsize::new(0));
    let count = cleaned.clone();
    let _actor = group
        .spawn(AktorNew {
            name: "already started".into(),
            kind: AktorKind::TokioThread,
            start: crate::AktorInit::Ready(async || ()),
            end: async move |_: ()| {
                count.fetch_add(1, Ordering::SeqCst);
            },
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        })
        .ready()
        .await
        .unwrap();
    let _reset = Reset;

    FAIL_SPAWN.with(|setting| setting.set(Some("aktor owner")));

    let attempt = group.spawn(AktorNew {
        name: "cannot create thread".into(),
        kind: AktorKind::TokioThread,
        start: crate::AktorInit::Try(async || -> Result<(), ((), String)> {
            panic!("setup ran after thread creation failed")
        }),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: core::num::NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    });
    let (error, _) = attempt.ready().await.err().unwrap();

    assert_eq!(error.reason(), AktorStartReason::InvalidSetup);
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}
