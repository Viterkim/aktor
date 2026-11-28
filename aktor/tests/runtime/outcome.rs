#![cfg(feature = "testing")]
use super::*;
use core::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::oneshot;

struct Notice(Arc<AtomicUsize>, bool);
impl Drop for Notice {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);

        if self.1 {
            panic!("queued destructor");
        }
    }
}

#[tokio::test]
async fn settle_before_cleanup() {
    for force in [false, true] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let count = dropped.clone();
        let (end, cleaned) = oneshot::channel();
        let actors = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "settlement".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Ready(async || ()),
                end: move |_| end.send(count.load(Ordering::SeqCst)).unwrap(),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(2).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_millis(100),
        });
        actors.ready().await.unwrap();

        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        call(actors.handles(), async move |_| {
            entered.send(()).unwrap();
            released.await.unwrap();
            panic!("operation failed");
        })
        .cast()
        .await
        .unwrap();
        entering.await.unwrap();

        let first = Notice(dropped.clone(), true);
        call(actors.handles(), async move |_| drop(first))
            .cast()
            .await
            .unwrap();
        let second = Notice(dropped, false);
        let mut discarded = call(actors.handles(), async move |_| drop(second))
            .send()
            .await
            .unwrap();

        if force {
            actors.killswitch().stop();
        } else {
            release.send(()).unwrap();
        }
        let _ = actors.completion().await.unwrap();

        assert_eq!(cleaned.await.unwrap(), 2);

        assert!(matches!(
            discarded.try_take().unwrap(),
            Err(aktor::AktorDeliveryError {
                outcome: message::AktorDeliveryOutcome::Discarded,
                ..
            })
        ));
    }
}

#[tokio::test]
async fn discarded_call_leaves_end_failure_unread() {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "retiring".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || ()),
            end: async |_| Err(((), String::from("end failed"))),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::MIN,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(1),
    });
    actors.ready().await.unwrap();
    let (entered, running) = oneshot::channel();
    let (release, released) = oneshot::channel();

    call(actors.handles(), async move |_| {
        entered.send(()).unwrap();
        released.await.unwrap();
        aktor_retire();
    })
    .cast()
    .await
    .unwrap();
    running.await.unwrap();

    let queued = call(actors.handles(), async |_| ()).send().await.unwrap();
    release.send(()).unwrap();
    let report = actors
        .completion()
        .wait_retired()
        .await
        .unwrap()
        .into_report();

    assert_eq!(queued.await.unwrap_err().failure_id, None);
    assert!(!report.failures[0].delivered());
}
