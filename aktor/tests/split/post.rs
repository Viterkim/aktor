use super::Database;
use aktor::*;

#[aktor]
pub async fn row(database: &mut Database, value: String) -> usize {
    database.rows.push(value);

    database.rows.len() - 1
}
