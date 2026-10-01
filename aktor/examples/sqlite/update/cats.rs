use crate::{CatId, OwnerId, Result};
use aktor::*;
use rusqlite::{Connection, params};

#[aktor]
pub async fn rename(db: &Connection, id: CatId, name: impl AsRef<str>) -> Result<()> {
    db.execute(
        "UPDATE cat SET name = ? WHERE id = ?",
        params![name.as_ref(), id.0],
    )?;

    Ok(())
}

#[aktor]
pub async fn adopt(db: &Connection, id: CatId, owner: OwnerId) -> Result<()> {
    let changed = db.execute(
        "UPDATE cat SET owner = ? WHERE id = ?",
        params![owner.0, id.0],
    )?;

    if changed == 0 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }

    Ok(())
}
