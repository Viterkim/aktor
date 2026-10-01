use super::StorageError;
use aktor::*;
use rusqlite::{Connection, OptionalExtension};

#[aktor]
pub async fn read(db: &Connection, key: String) -> Result<String, StorageError> {
    db.query_row(
        "SELECT value FROM preferences WHERE key = ?1",
        [&key],
        |row| row.get(0),
    )
    .optional()?
    .ok_or(StorageError::Missing(key))
}
