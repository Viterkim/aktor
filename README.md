# Aktor, not an actor framework

Something that makes a single owner of a resource, that does a 'blocking response' to each request one after the other with automatic messages/queue/boilerplate.

Write some sql in a normal function, call it somewhere with `.await`, and 'go to definition' in your editor goes to that exact function, not some boilerplate Request / Response giant enum, which has 85 variants you cant even hit. To try to get the whole 1 function with input -> 1 output at a type level.

It basically just swaps out the first argument of the function for your handle, and does the plumbing for you.

And without being forced to have some giant impl or implement some traits and set up associated types manually.

You can do whichever structure you want, with functions living in like `things::get::users()` and `other::write::something()` etc. Just write `#[aktor]` above your function.

```toml
[dependencies]
aktor = { version = "0.0.2", features = ["tokio"] }
rusqlite = { version = "0.40", features = ["bundled"] }
```

Anything up to 0.1 will not have a stable api.

## Example

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

That queues the whole transaction once, the calls inside it run right there. Passing the handle again would queue them behind yourself.

## Opening / starting / spawning

Setup and cleanup are just closures. Give them the text you want printed if something fails:

```rust
let setup = || {
    let e = |error: rusqlite::Error| AktorSetupError::new(error.to_string());
    let db = Connection::open("users.sqlite").map_err(e)?;

    db.execute("CREATE TABLE IF NOT EXISTS user (name TEXT NOT NULL)", []).map_err(e)?;

    Ok(db)
};
let cleanup = |db: Connection| {
    db.close().map_err(|(_, error)| AktorCleanupError::new(error.to_string()))
};
```

Then start the group and carry on with ordinary startup:

```rust
let mut actors = AktorGroup::new();
let kill = actors.killswitch();
let closing = actors.start()?;

let database = actors.spawn(ActorArgs::new("users", setup, cleanup)).await?;

// Start your UI / tasks with database.
// Put kill.stop() in your Close / Ctrl+C handler.

kill.wait_stopping().await;
let report = closing.await;
```

If an actor dies, kill.wait_stopping() wakes up too, hook that into the same Close handler. Save settings still in your UI before stopping the group, and stop your own tasks before leaving the runtime.

Use start_with(after) for your code at the end, [like this](aktor/docs/examples.md#closing-the-application). actors.shutdown().await starts closing and waits for the report.

For a struct you already have, actors.spawn_value("counter", counter).await? is enough. database.new_handle() gives you another handle to the same queue.

The group gets five seconds to close, AktorGroup::with_grace(duration) changes that. The [Ctrl+C example](aktor/examples/group.rs) shows an app closing, [runtime notes](aktor/docs/runtime.md) cover stopping your tasks.

## Setups

### Browser workers

```toml
aktor = { version = "0.0.2", features = ["wasm_browser_workers"] }
```

Runs in Web Workers, [browser readme](integrations/worker/README.md) has a SQLite example.

### Embassy + allocator

```toml
aktor = { version = "0.0.2", features = ["embassy"] }
```

[Embassy readme](integrations/embassy/README.md)

This is the local backend, it also runs in [plain WASM hosts](integrations/wasm/README.md).

## Docs

[Extra examples](aktor/docs/examples.md)

[Pause and shutdown](aktor/docs/runtime.md)

[Comparisons](aktor/docs/compare-libs.md)

[Docs.rs](https://docs.rs/aktor/latest/aktor/) / [Crates.io](https://crates.io/crates/aktor)
