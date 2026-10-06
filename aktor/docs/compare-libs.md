# Simple Actor Comparison

One SQLite connection on its own thread, queries in normal files:

```rust
let id = post::cat(&database, "Bingo".into()).await?;
let cat = get::cat(&database, id).await?;
```

Go to post::cat and find the query. That's the thing i care about here.

## asyncified / tokio-rusqlite

[asyncified 0.6.2](https://docs.rs/asyncified/0.6.2/asyncified/) and [tokio-rusqlite 0.8.0](https://docs.rs/tokio-rusqlite/0.8.0/tokio_rusqlite/) wrap the query in a closure:

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

## actify / interthread

[Actify 0.9.0](https://docs.rs/actify/0.9.0/actify/) and [interthread 3.1.0](https://docs.rs/interthread/3.1.0/interthread/) generate handles from annotated impls:

```rust
let id = database.add_cat("Bingo".into()).await?;
```

## Message APIs

[Kameo 0.22.2](https://docs.rs/kameo/0.22.2/kameo/) takes messages:

```rust
let id = database.ask(AddCat { name: "Bingo".into() }).await?;
```

[Actix 0.13.5](https://docs.rs/actix/0.13.5/actix/trait.Handler.html) uses message types with Handler implementations. [act-zero 0.4.0](https://docs.rs/act-zero/0.4.0/act_zero/) uses actor methods with ActorResult and call!(...).

## aktor

In post.rs:

```rust
#[aktor]
pub async fn cat(connection: &Connection, name: String) -> rusqlite::Result<i64> {
    connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

    Ok(connection.last_insert_rowid())
}
```

Functions live wherever you want and return their own output. Set up the connection with TokioThread, then call post::cat(&actors.handles, name).await. [Setup example](../../README.md#counter).
