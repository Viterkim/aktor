# Aktor, not an actor framework

Something that makes a single owner of a resource, that does a 'blocking response' to each request one after the other with automatic messages/queue/boilerplate.

Write some sql in a normal function, call it somewhere with `.await`, and 'go to definition' in your editor goes to that exact function, not some boilerplate Request / Response giant enum, which has 85 variants you cant even hit. To try to get the whole 1 function with input -> 1 output at a type level.

It basically just swaps out the first argument of the function for your handle, and does the plumbing for you.

And without being forced to have some giant impl or implement some traits and set up associated types manually.

You can do whichever structure you want, with functions living in like `things::get::users()` and `other::write::something()` etc. Just write `#[aktor]` above your function.

```toml
[dependencies]
aktor = { version = "0.0.2", features = ["tokio"] }
```

Anything up to 0.1 will not have a stable api.

## Example

```toml
aktor = { version = "0.0.2", features = ["tokio"] }
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

Keep the actors with the application:

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

let after = async |report: ShutdownReport| {
    if report.failed() { eprintln!("{report}"); }

    Ok::<_, AktorCleanupError>(())
};

let run = async move |app: &mut AktorGroup| -> Result<(), AktorError> {
    let database = app.spawn(ActorArgs {
        name: "users".into(),
        capacity: 32,
        setup,
        cleanup,
    }).await.map_err(|error| AktorError::new(error.to_string()))?;

    run_application(database).await.map_err(|error| AktorError::new(error.to_string()))?;
    Ok(())
};

let application = AktorGroup::new();
let kill = application.killswitch();

let outcome = application.run(run, after).await;
```

It gets its own thread, and capacity is how many calls can wait in the queue. That means that callers will wait if its full. Errors from your function come back as usual.

Put `kill.stop()` in your Ctrl+C or Close handler. If an actor dies it starts closing too, each actor cleans up its own resource, then the last closure gets the reports.

If the database dies, we're closing the app anyway, your queries don't need another Result for that.

Setup and cleanup return the text you want printed, [you can keep the error itself too](aktor/docs/examples.md#setup--cleanup-errors). With er, use `error.er_report_string()`.

The group gets five seconds to close by default, `AktorGroup::with_grace(duration)` changes that. Your own spawned tasks still need closing too, see the [runtime notes](aktor/docs/runtime.md#application-shutdown).

The [group example](aktor/examples/group.rs) hooks up Ctrl+C and flushes its file before the application cleanup runs. The low level `Aktor::spawn` is still available if you need to own the lifetime yourself.

## Setups

### Tokio

```toml
[dependencies]
aktor = { version = "0.0.2", features = ["tokio"] }
```

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

### Repo links

[Extra examples](aktor/docs/examples.md)

[Runtime details](aktor/docs/runtime.md)

[Comparisons](aktor/docs/compare-libs.md)

[GitHub Repo](https://github.com/Viterkim/aktor)

### External links

[Docs.rs](https://docs.rs/aktor/latest/aktor/)

[Crates.io for the lib](https://crates.io/crates/aktor)

[Crates.io for the macros](https://crates.io/crates/aktor-macros/)

[Libs.rs](https://lib.rs/crates/aktor)
