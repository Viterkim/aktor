use aktor::*;
use core::num::NonZeroUsize;
use er::*;
use std::time::Duration;

#[derive(Default)]
pub struct Connection {
    pub rows: Vec<String>,
}

#[aktor]
pub async fn insert(connection: &mut Connection, value: String) -> usize {
    connection.rows.push(value);

    connection.rows.len() - 1
}

#[aktor]
pub async fn find(connection: &Connection, id: usize) -> Option<String> {
    connection.rows.get(id).cloned()
}

#[aktor]
pub async fn count(connection: &Connection) -> usize {
    connection.rows.len()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ErMain {
    let started = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "database".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Ready(async || Connection::default()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    started
        .ready()
        .await
        .er_val(|(_, report)| ErMainError.er_save("startup", report))?;
    let database = started.handles();

    let inserted = insert(database, "BingoManden".to_owned())
        .send()
        .await
        .unwrap();
    let rows: usize = count(database).await.er(())?;

    let id: usize = inserted.await.er(())?;
    let row: Option<String> = find(database, id).await.er(())?;

    assert_eq!(row.as_deref(), Some("BingoManden"));
    assert_eq!(rows, 1);

    started.stop().await.into_result().er(())?;

    Ok(())
}
