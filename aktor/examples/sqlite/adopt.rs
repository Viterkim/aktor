use crate::{CatId, OwnerId, Result, post, update};
use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn household(
    db: &mut Connection,
    name: impl AsRef<str>,
    cats: Vec<CatId>,
) -> Result<OwnerId> {
    let transaction = db.transaction()?;
    let owner = post::owners::create(&*transaction, name.as_ref()).await?;

    for cat in cats {
        update::cats::adopt(&*transaction, cat, owner).await?;
    }

    transaction.commit()?;

    Ok(owner)
}
