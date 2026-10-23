use aktor::{AktorGroup, aktor};
use rusqlite::Connection;

#[cfg(not(test))]
type Database = aktor::Aktor<Connection>;
#[cfg(test)]
type Database = aktor::testing::Mock<Connection>;

struct App {
    database: Database,
}
impl App {
    async fn add_user(&self, name: String) -> rusqlite::Result<String> {
        let id = insert(&self.database, name).await?;
        load(&self.database, id).await
    }
}

#[aktor]
async fn insert(database: &Connection, name: String) -> rusqlite::Result<i64> {
    database.execute("INSERT INTO user (name) VALUES (?)", [name])?;
    Ok(database.last_insert_rowid())
}

#[aktor]
async fn load(database: &Connection, id: i64) -> rusqlite::Result<String> {
    database.query_row("SELECT name FROM user WHERE rowid = ?", [id], |row| {
        row.get(0)
    })
}

#[cfg(not(test))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut actors = AktorGroup::new();
    let _completion = actors.start_with(|_| {})?;
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE user (name TEXT)")?;
    let app = App {
        database: actors.spawn_value("sqlite", connection).await?,
    };

    let result = app.add_user("Katten".into()).await;
    let report = actors.shutdown().await;
    if report.failed() {
        return Err(aktor::AktorRunError::from(report).into());
    }

    println!("{}", result?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aktor::testing::Preset;

    fn successful() -> Preset<Connection> {
        Preset::new()
            .on(insert::handler(async |_| Ok(7)))
            .on(load::handler(async |_| Ok("Katten".into())))
    }

    #[tokio::test]
    async fn load_failure() {
        let mut actors = AktorGroup::new();
        let _completion = actors.start_with(|_| {}).unwrap();
        let database = successful()
            .on(load::handler(async |_| Err(rusqlite::Error::InvalidQuery)))
            .start(&mut actors)
            .await
            .unwrap();
        let app = App { database };

        assert!(app.add_user("Katten".into()).await.is_err());

        let report = actors.shutdown().await;
        assert!(!report.failed(), "{report}");
    }
}
