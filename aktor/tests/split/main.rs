#![cfg(feature = "macros")]

use aktor::listener::spawn_local;
use std::error::Error;

#[path = "get.rs"]
pub mod get;
#[path = "post.rs"]
pub mod post;

#[derive(Default)]
pub struct Database {
    pub rows: Vec<String>,
}

#[tokio::test]
async fn modules() -> Result<(), Box<dyn Error>> {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let mut local = Database::default();
            assert_eq!(post::row(&mut local, "local".into()).await, 0);

            let (database, actor) = spawn_local(&executor, Database::default(), 8)?;

            let id = post::row(&database, "BingoManden".into()).await;
            assert_eq!(
                get::row(&database, id).await.as_deref(),
                Some("BingoManden")
            );

            drop(database);
            assert_eq!(actor.await?.rows.len(), 1);

            Ok(())
        })
        .await
}
