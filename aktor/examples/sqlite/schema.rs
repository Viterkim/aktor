use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn create(connection: &mut Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
        CREATE TABLE owner (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE cat (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            owner INTEGER REFERENCES owner(id)
        )",
    )?;

    Ok(())
}
