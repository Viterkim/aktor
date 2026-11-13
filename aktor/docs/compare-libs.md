# Queries as functions

Put a SQLite connection on its own thread. The calls can look the same with either library.

```rust
let id = post::cat(&database, "Bingo".into()).await?;
let cat = get::cat(&database, id).await?;
```

Go to `post::cat` and find the SQL. That's the bit i care about. Now try using that same insert inside a transaction.

With tokio-rusqlite, `call` sends a closure to the connection's thread. Either the query can fail or the thread can already be closed. [Connection::call](https://docs.rs/tokio-rusqlite/0.9.0/tokio_rusqlite/struct.Connection.html#method.call)

```rust
pub async fn cat(
    database: &tokio_rusqlite::Connection,
    name: String,
) -> tokio_rusqlite::Result<i64> {
    database.call(move |connection| {
        connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

        Ok(connection.last_insert_rowid())
    })
    .await
}
```

With Aktor, the function itself takes the SQLite connection.

```rust
#[aktor]
pub async fn cat(connection: &Connection, name: String) -> rusqlite::Result<i64> {
    connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

    Ok(connection.last_insert_rowid())
}
```

A handle queues the insert. Inside a transaction, `post::cat(&*transaction, name).await?` runs the very same function on the connection you already have. With tokio-rusqlite you'd pull the SQL into its own function and call it from the closure and the transaction.
