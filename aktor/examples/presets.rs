#[cfg(not(test))]
use aktor::{AktorKind, AktorNew, AktorStuck};
use aktor::{AktorSetup, aktor, aktor_start};
use er::*;
use rusqlite::Connection;
use std::time::Duration;

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
async fn main() -> ErMain {
    let started = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "sqlite".into(),
            kind: AktorKind::TokioThread,
            start: async || open(),
            end: (),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    })
    .await
    .er_test()?;

    let app = App {
        database: started.handles,
    };

    println!("{:?}", app.add_user("Katten".into()).await);
    started.group.shutdown().await.into_result().er_test()?;

    Ok(())
}

#[cfg(not(test))]
#[derive(Er)]
struct OpenErr {
    step: &'static str,
}

#[cfg(not(test))]
fn open() -> ErResult<Connection, OpenErr> {
    let connection = Connection::open_in_memory().er("open")?;
    connection
        .execute_batch("CREATE TABLE user (name TEXT)")
        .er("create table")?;

    Ok(connection)
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
    async fn load_failure() -> ErTest {
        let started = aktor_start(AktorSetup {
            actors: successful().on(load::handler(async |_| Err(rusqlite::Error::InvalidQuery))),
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        })
        .await
        .er_test()?;

        let app = App {
            database: started.handles,
        };

        er_assert_err!(
            app.add_user("Katten".into()).await,
            rusqlite::Error::InvalidQuery
        );
        started.group.shutdown().await.into_result().er_test()?;

        Ok(())
    }
}
