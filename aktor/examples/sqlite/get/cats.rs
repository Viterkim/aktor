use crate::{Cat, CatId, OwnerId, Result};
use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn by_id(db: &Connection, id: CatId) -> Result<Cat> {
    db.query_row("SELECT name, owner FROM cat WHERE id = ?", [id.0], |row| {
        Ok(Cat {
            id,
            name: row.get(0)?,
            owner: row.get::<_, Option<i64>>(1)?.map(OwnerId),
        })
    })
}

#[aktor]
pub async fn by_owner(db: &Connection, owner: OwnerId) -> Result<Vec<CatId>> {
    db.prepare("SELECT id FROM cat WHERE owner = ? ORDER BY id")?
        .query_map([owner.0], |row| row.get(0).map(CatId))?
        .collect()
}

#[aktor]
pub async fn count(db: &Connection) -> Result<i64> {
    db.query_row("SELECT COUNT(*) FROM cat", [], |row| row.get(0))
}
