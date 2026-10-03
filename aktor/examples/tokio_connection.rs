use aktor::*;

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
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let setup = || Ok::<_, aktor::AktorError>(Connection::default());
    let cleanup = |_| Ok::<_, aktor::AktorError>(());

    let database = Aktor::spawn(SpawnArgs {
        name: "database".into(),
        capacity: 128,
        failure: FailurePolicy::Unwind,
        setup,
        cleanup,
    })
    .await?;

    let inserted = insert(&database, "BingoManden".to_owned()).send().await;
    let rows: usize = count(&database).await;

    let id: usize = inserted.await;
    let row: Option<String> = find(&database, id).await;

    assert_eq!(row.as_deref(), Some("BingoManden"));
    assert_eq!(rows, 1);

    database.shutdown().await?;

    Ok(())
}
