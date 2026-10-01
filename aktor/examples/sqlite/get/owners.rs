use crate::{Owner, OwnerId, Result};
use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn by_id(db: &Connection, id: OwnerId) -> Result<Owner> {
    db.query_row("SELECT name FROM owner WHERE id = ?", [id.0], |row| {
        Ok(Owner {
            id,
            name: row.get(0)?,
        })
    })
}

#[aktor]
pub async fn by_name(db: &Connection, name: impl AsRef<str>) -> Result<OwnerId> {
    db.query_row(
        "SELECT id FROM owner WHERE name = ?",
        [name.as_ref()],
        |row| row.get(0).map(OwnerId),
    )
}

#[aktor]
pub async fn count(db: &Connection) -> Result<i64> {
    db.query_row("SELECT COUNT(*) FROM owner", [], |row| row.get(0))
}
