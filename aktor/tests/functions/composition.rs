use super::*;
use rusqlite::Connection;

mod queries {
    use super::*;

    #[aktor]
    pub async fn insert(db: &Connection, name: String) -> rusqlite::Result<i64> {
        db.execute("INSERT INTO users(name) VALUES (?1)", [name])?;

        Ok(db.last_insert_rowid())
    }

    #[aktor]
    pub async fn name(db: &Connection, id: i64) -> rusqlite::Result<String> {
        db.query_row("SELECT name FROM users WHERE id = ?1", [id], |row| {
            row.get(0)
        })
    }
}

fn open() -> rusqlite::Result<Connection> {
    let db = Connection::open_in_memory()?;

    db.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)", [])?;

    Ok(db)
}

#[aktor]
async fn transaction(db: &mut Connection, name: String, commit: bool) -> rusqlite::Result<String> {
    let tx = db.transaction()?;
    let id = queries::insert(&*tx, name).await?;
    let name = queries::name(&*tx, id).await?;

    if commit {
        tx.commit()?;
    }

    Ok(name)
}

#[tokio::test]
async fn local() {
    let mut db = open().unwrap();

    assert_eq!(
        transaction(&mut db, "rollback".into(), false)
            .await
            .unwrap(),
        "rollback"
    );

    assert_eq!(
        db.query_row::<i64, _, _>("SELECT count(*) FROM users", [], |row| row.get(0))
            .unwrap(),
        0
    );

    assert_eq!(
        transaction(&mut db, "kept".into(), true).await.unwrap(),
        "kept"
    );
    assert_eq!(queries::name(&db, 1).await.unwrap(), "kept");
}

#[tokio::test]
async fn remote() {
    let (db, task) = spawn_thread(open().unwrap(), 1).unwrap();

    assert_eq!(
        transaction(&db, "actor".into(), true).await.unwrap(),
        "actor"
    );
    assert_eq!(queries::name(&db, 1).await.unwrap(), "actor");

    drop(db);
    task.join_async().await.unwrap();
}
