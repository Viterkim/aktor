use aktor::*;
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
        actors: AktorNew {
            name: "database".into(),
            kind: AktorKind::TokioThread,
            start: async || aktor_ready(Connection::default()),
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |report| {
            if report.failed() {
                eprintln!("{report}");
            }
        },
        grace: Duration::from_secs(5),
    })
    .await
    .er_report()?;
    let database = &started.handles;

    let inserted = insert(database, "BingoManden".to_owned()).send().await;
    let rows: usize = count(database).await;

    let id: usize = inserted.await;
    let row: Option<String> = find(database, id).await;

    assert_eq!(row.as_deref(), Some("BingoManden"));
    assert_eq!(rows, 1);

    started.shutdown().await;

    Ok(())
}
