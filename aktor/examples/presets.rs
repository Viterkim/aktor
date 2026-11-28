#[cfg(not(test))]
use aktor::{AktorHooks, AktorKind, AktorNew, AktorStuck};
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
#[derive(Er)]
struct AddUserErr;

impl App {
    async fn add_user(&self, name: String) -> ErResult<String, AddUserErr> {
        let id = insert(&self.database, name).await.er(())?.er(())?;
        load(&self.database, id).await.er(())?.er(())
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
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "sqlite".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Try(async || open()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    started
        .ready()
        .await
        .er_val(|(_, report)| ErMainError.er_save("startup", report))?;

    let app = App {
        database: started.handles().clone(),
    };

    println!("{}", app.add_user("Katten".into()).await.er(())?);
    started.group().stop().await.into_result().er(())?;

    Ok(())
}

#[cfg(not(test))]
#[derive(Er)]
struct OpenErr {
    step: &'static str,
}

#[cfg(not(test))]
fn open() -> Result<Connection, (OpenErr, String)> {
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

    #[tokio::test]
    async fn load_failure() -> ErTest {
        let started = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: Preset::new([
                insert::handler(async |_| Ok(7)),
                load::handler(async |_| Err(rusqlite::Error::InvalidQuery)),
            ]),
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });

        let app = App {
            database: started.handles().clone(),
        };

        let result = app.add_user("Katten".into()).await;
        let error = er_assert_err!(result);
        assert!(matches!(
            error.er_find::<rusqlite::Error>(),
            Some(rusqlite::Error::InvalidQuery)
        ));
        started.group().stop().await.into_result().er_test()?;

        Ok(())
    }
}
