use crate::{CatId, Result};
use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn create(db: &Connection, name: impl AsRef<str>) -> Result<CatId> {
    db.execute("INSERT INTO cat (name) VALUES (?)", [name.as_ref()])?;

    Ok(CatId(db.last_insert_rowid()))
}
