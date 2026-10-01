use super::Database;
use aktor::*;

#[aktor]
pub async fn row(database: &Database, id: usize) -> Option<String> {
    database.rows.get(id).cloned()
}
