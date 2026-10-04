# Aktor, not an actor framework

Write some sql in a normal function, pass your handle and `.await` it. One owner runs the calls one after the other, 'go to definition' takes you to the function you wrote.

```toml
[dependencies]
aktor = { version = "0.0.3", features = ["tokio"] }
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

Start a group and give it your connection:

```rust
let db = Connection::open("users.sqlite")?;
db.execute("CREATE TABLE IF NOT EXISTS user (name TEXT NOT NULL)", [])?;

let mut actors = AktorGroup::new();
actors.start()?;
let database = actors.spawn_value("users", db).await?;

let id = insert_user(&database, "Katten".into()).await?;

// your app does its thing
let report = actors.shutdown().await;
```

spawn_value moves db onto its own thread, "users" is the name shown in reports. Your function's errors come back as usual, database.new_handle() gives you another handle to the same queue.

Functions can live wherever you want, like `things::get::users()` and `other::write::something()`.

Inside another query, pass the connection you already have and it runs right there. Passing the handle again queues it behind yourself. The [examples](aktor/docs/examples.md) show transactions and your own setup / cleanup.

For your Close / Ctrl+C handler, get actors.killswitch() and call stop(). Actor failure wakes kill.wait_stopping() too, hook it into that same handler. Stop your application's tasks and await the report before leaving the runtime, the [group example](aktor/examples/group.rs) shows that part.

## Other setups

Browser: use the wasm_browser_workers feature, the [browser readme](integrations/worker/README.md) has a SQLite example.

Embassy + allocator: use the embassy feature, [like this](integrations/embassy/README.md). It also runs in [plain WASM hosts](integrations/wasm/README.md).

## Docs

[Extra examples](aktor/docs/examples.md)

[Pause and shutdown](aktor/docs/runtime.md)

[Comparisons](aktor/docs/compare-libs.md)

[Docs.rs](https://docs.rs/aktor/latest/aktor/) / [Crates.io](https://crates.io/crates/aktor)
