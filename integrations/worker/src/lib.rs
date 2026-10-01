#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod browser;
pub mod preferences;

#[cfg(test)]
mod tests {
    use super::preferences::{StorageError, read::read, write::write};
    use aktor::listener::spawn_async;
    use aktor::*;
    use rusqlite::Connection;

    #[tokio::test]
    async fn preferences() {
        let (db, actor, thread) = spawn_async(SpawnArgs {
            name: "preferences".into(),
            capacity: 2,
            failure: FailurePolicy::Unwind,
            setup: async || {
                let db = Connection::open_in_memory()?;
                db.execute(
                    "CREATE TABLE preferences(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
                    [],
                )?;

                Ok::<_, rusqlite::Error>(db)
            },
            cleanup: async |db: Connection| db.close().map_err(|(_, error)| error),
        })
        .await
        .unwrap();

        assert_eq!(
            write(&db, "volume".into(), "0.7".into()).await.unwrap(),
            "0.7"
        );
        assert_eq!(read(&db, "volume".into()).await.unwrap(), "0.7");
        assert_eq!(
            read(&db, "absent".into()).await,
            Err(StorageError::Missing("absent".into()))
        );

        actor.shutdown().await.unwrap();
        thread.join_async().await.unwrap().unwrap();
    }
}

#[cfg(all(feature = "wrong-role", target_family = "wasm", target_os = "unknown"))]
mod wrong_role {
    use aktor::*;

    struct Database;
    struct Session;

    #[aktor(actor = Database)]
    async fn read(state: &u32) -> u32 {
        *state
    }

    async fn mismatched(worker: &aktor::worker::Worker<u32, Session>) {
        read(worker).await;
    }
}
