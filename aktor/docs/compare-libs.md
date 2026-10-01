# Simple Actor Comparison

One SQLite connection on its own thread. Add a cat, then get it back. I want the queries in normal files:

```text
database/
  get.rs
  post.rs
  schema.rs
```

And call them like this:

```rust
let id = post::cat(&database, "Bingo".into()).await?;
let cat = get::cat(&database, id).await?;
```

Go to post::cat and find the query. That's the thing i care about here.

## asyncified (0.6.2)

[Docs](https://docs.rs/asyncified/0.6.2/asyncified/)

A normal function in post.rs:

```rust
pub async fn cat(
    database: &Asyncified<Connection>,
    name: String,
) -> rusqlite::Result<i64> {
    database
        .call(move |connection| {
            connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

            Ok(connection.last_insert_rowid())
        })
        .await
}
```

The query lives where you want it and returns its own type. Every function still needs that .call(move |connection| ...) around its body.

## tokio-rusqlite (0.8.0)

[Docs](https://docs.rs/tokio-rusqlite/0.8.0/tokio_rusqlite/)

```rust
pub async fn cat(
    database: &tokio_rusqlite::Connection,
    name: String,
) -> tokio_rusqlite::Result<i64> {
    database
        .call(move |connection| {
            connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

            Ok(connection.last_insert_rowid())
        })
        .await
}
```

Opens SQLite on its own thread for you. Same closure around every query.

## actify (0.9.0)

[Docs](https://docs.rs/actify/0.9.0/actify/)

```rust
#[actify]
impl Database {
    pub fn add_cat(&mut self, name: String) -> rusqlite::Result<i64> {
        self.connection
            .execute("INSERT INTO cat (name) VALUES (?)", [name])?;

        Ok(self.connection.last_insert_rowid())
    }
}

let id = database.add_cat("Bingo".into()).await?;
```

Methods on a generated handle. You can split the impl across files, extra blocks need a name for their handle trait:

```rust
#[actify(name = "DatabaseGetters")]
impl Database {
    pub fn cat(&mut self, id: i64) -> rusqlite::Result<Option<Cat>> {
        // query goes here
    }
}
```

I want get::cat and post::cat as free functions.

## act-zero (0.4.0)

[Docs](https://docs.rs/act-zero/0.4.0/act_zero/)

```rust
impl Database {
    pub async fn add_cat(&mut self, name: String) -> ActorResult<i64> {
        self.connection
            .execute("INSERT INTO cat (name) VALUES (?)", [name])?;

        Produces::ok(self.connection.last_insert_rowid())
    }
}

let id = call!(database.add_cat("Bingo".into())).await?;
```

Actor methods with ActorResult and call!(...). It works across executors, isolating blocking SQLite is up to you.

## The bigger actor crates

[Kameo 0.22.2](https://docs.rs/kameo/0.22.2/kameo/) can generate message types, then you call them like:

```rust
let id = database.ask(AddCat { name: "Bingo".into() }).await?;
```

[Actix 0.13.5](https://docs.rs/actix/0.13.5/actix/trait.Handler.html) has explicit message types and Handler implementations.

[interthread 3.1.0](https://docs.rs/interthread/3.1.0/interthread/) starts from an annotated impl and generates a handle:

```rust
let id = database.add_cat("Bingo".into()).await?;
```

## aktor

In post.rs:

```rust
use aktor::*;
use rusqlite::Connection;

#[aktor]
pub async fn cat(
    connection: &mut Connection,
    name: String,
) -> rusqlite::Result<i64> {
    connection.execute("INSERT INTO cat (name) VALUES (?)", [name])?;

    Ok(connection.last_insert_rowid())
}
```

And elsewhere:

```rust
let database = Aktor::spawn(SpawnArgs {
    name: "sqlite".into(),
    capacity: 128,
    failure: FailurePolicy::Unwind,
    setup,
    cleanup,
})
.await?;

let id = post::cat(&database, "Bingo".into()).await?;
let cat = get::cat(&database, id).await?;
```

Pass the connection you already have for nested calls, including inside a transaction. Functions can live wherever you want.
