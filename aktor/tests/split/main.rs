#![cfg(feature = "macros")]

use aktor::*;
use core::num::NonZeroUsize;
use std::time::Duration;

#[path = "get.rs"]
pub mod get;
#[path = "post.rs"]
pub mod post;

#[derive(Default)]
pub struct Database {
    pub rows: Vec<String>,
}

#[tokio::test]
async fn modules() {
    let mut local = Database::default();

    assert_eq!(post::row(&mut local, "local".into()).await, 0);

    let started = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "database".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || Database::default()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    let id = post::row(started.handles(), "BingoManden".into())
        .await
        .unwrap();

    assert_eq!(
        get::row(started.handles(), id).await.unwrap().as_deref(),
        Some("BingoManden")
    );
}
