# Aktor, not an actor framework

Something that makes a single owner of a resource, that does a 'blocking response' to each request one after the other with automatic messages/queue/boilerplate.

Write some sql in a normal function, call it somewhere with `.await`, and 'go to definition' in your editor goes to that exact function, not some boilerplate Request / Response giant enum, which has 85 variants you cant even hit. To try to get the whole 1 function with input -> 1 output at a type level.

It basically just swaps out the first argument of the function for your handle, and does the plumbing for you.

And without being forced to have some giant impl or implement some traits and set up associated types manually.

You can do whichever structure you want, with functions living in like `things::get::users()` and `other::write::something()` etc. Just write `#[aktor]` above your function.

```toml
[dependencies]
aktor = "0.0.1"
```

Anything up to 0.1 will not have a stable api.

## Example

```toml
aktor = { version = "0.0.1", features = ["tokio"] }
rusqlite = { version = "0.40", features = ["bundled"] }
```

```rust
use aktor::*;
use rusqlite::{Connection, Result};

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])?;

    Ok(db.last_insert_rowid())
}
```

Then in your code:

```rust
let id = insert_user(&database, "Katten".into()).await?;
```

## Calling another query (nested)

Pass the 'thing' you already have (`Connection` here).

```rust
#[aktor]
pub async fn insert_users(db: &mut Connection, names: Vec<String>) -> Result<()> {
    let transaction = db.transaction()?;

    for name in names {
        insert_user(&*transaction, name).await?;
    }

    transaction.commit()
}
```

The [SQLite example](aktor/examples/sqlite/adopt.rs) also does this and shows it.

## Clone / new_handle

Explicitly make a new handle to the same actor and queue.

```rust
let another_handle = database.new_handle();
```

shutdown finishes the queued calls and closes it:

```rust
database.shutdown().await?;
```

## Opening / starting / spawning

Inside your async:

```rust
let database = Aktor::spawn(SpawnArgs {
    name: "users".into(),
    capacity: 32,
    failure: FailurePolicy::Unwind,
    setup: || {
        let db = Connection::open("users.sqlite")?;
        db.execute("CREATE TABLE IF NOT EXISTS user (name TEXT NOT NULL)", [])?;

        Ok::<_, rusqlite::Error>(db)
    },
    cleanup: |db: Connection| db.close().map_err(|(_, error)| error),
})
.await?;
```

It gets its own thread, and capacity is how many calls can wait in the queue. That means that callers will wait if its full. Errors from your function come back as usual.

## Setups

### Tokio

```toml
[dependencies]
aktor = { version = "0.0.1", features = ["tokio"] }
```

### Browser / webassembly

```toml
aktor = { version = "0.0.1", features = ["worker"] }
```

[Browser readme](integrations/worker/README.md)

### Embassy + allocator

```toml
aktor = { version = "0.0.1", features = ["embassy"] }
```

[Embassy readme](integrations/embassy/README.md)

## Docs

### Repo links

[Extra examples](aktor/docs/examples.md)

[Browser setup](integrations/worker/README.md) uses the worker feature with persistent SQLite in OPFS.

[Embassy setup](integrations/embassy/README.md) uses embassy with an allocator.

[Runtime details](aktor/docs/runtime.md)

[Comparisons](aktor/docs/compare-libs.md)

[GitHub Repo](https://github.com/Viterkim/aktor)

### External links

[Docs.rs](https://docs.rs/aktor/latest/aktor/)

[Crates.io for the lib](https://crates.io/crates/aktor)

[Crates.io for the macros](https://crates.io/crates/aktor-macros/)

[Libs.rs](https://lib.rs/crates/aktor)
