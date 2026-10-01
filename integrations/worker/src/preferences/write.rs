use super::StorageError;
use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn write(
    db: &mut Connection,
    key: String,
    value: String,
) -> Result<String, StorageError> {
    db.execute(
        "INSERT INTO preferences(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [&key, &value],
    )?;

    super::read::read(db, key).await
}
