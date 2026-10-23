# Queries as functions

One SQLite connection on its own thread, queries in normal files:

```rust
let id = post::cat(&database, "Bingo".into()).await?;
let cat = get::cat(&database, id).await?;
```

Go to post::cat and find the query. That's the thing i care about here.

With tokio-rusqlite the query goes in a closure:

```rust
pub async fn cat(
    database: &tokio_rusqlite::Connection,
    name: String,
) -> tokio_rusqlite::Result<i64> {
    database.call(move |connection| {
        connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

        Ok(connection.last_insert_rowid())
    }).await
}
```

Aktor keeps it in post.rs as a normal function:

```rust
#[aktor]
pub async fn cat(connection: &Connection, name: String) -> rusqlite::Result<i64> {
    connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

    Ok(connection.last_insert_rowid())
}
```

The handle queues it, passing the connection runs it right there. That lets another query call it inside a transaction too.
