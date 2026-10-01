use crate::{OwnerId, Result};
use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn create(db: &Connection, name: impl AsRef<str>) -> Result<OwnerId> {
    db.execute("INSERT INTO owner (name) VALUES (?)", [name.as_ref()])?;

    Ok(OwnerId(db.last_insert_rowid()))
}
