# Aktor, not an actor framework

Write some sql in a normal function, pass your handle and `.await` it. One owner runs the calls one after the other, 'go to definition' takes you to the function you wrote.

This checkout has the next setup API. [0.0.3's docs](https://docs.rs/aktor/0.0.3/aktor/) cover the published version.

```toml
[dependencies]
aktor = { path = "aktor", features = ["tokio"] }
```

Anything up to 0.1 will not have a stable api.

## Counter

```rust
use aktor::*;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;
    *count
}

let aktor_setup = AktorSetup {
    name: AktorName::new("counter"),
    role: AktorNoRole,
    kind: AktorKind::TokioThread,
    closures: AktorClosures::new(async || Ok(0_u32)),
    options: None,
};
let actors = aktor::start(aktor_setup).await?;

let count = add(&actors.handles, 5).await;
let report = actors.shutdown().await;
```

Aktor makes the serving loop. TokioThread owns its state on a dedicated thread, options: None gives you a queue of 32 and five seconds to shut down. The [counter example](aktor/examples/counter.rs) adds logging around each call.

## SQLite

```rust
use aktor::*;
use rusqlite::{Connection, Result};

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])?;

    Ok(db.last_insert_rowid())
}
```

Create the connection in your start closure, then call it:

```rust
let id = insert_user(&database, "Katten".into()).await?;
```

The [SQLite example](aktor/examples/sqlite/main.rs) has the setup and cleanup. Your function's errors come back as usual, database.new_handle() gives you another handle to the same queue. Names show up in reports, roles can restrict which operations a handle accepts.

Functions can live wherever you want, like `things::get::users()` and `other::write::something()`.

Inside another query, pass the connection you already have and it runs right there. Passing the handle again queues it behind yourself. The [examples](aktor/docs/examples.md) show transactions and your own setup / cleanup.

For your Close / Ctrl+C handler, get actors.killswitch() and call stop(). Actor failure wakes kill.wait_stopping() too, hook it into that same handler. Stop your application's tasks and await actors.shutdown() before leaving the runtime.

## Other setups

Browser: use the wasm_browser_workers feature, the [browser readme](integrations/worker/README.md) has a SQLite example.

Embassy + allocator: use embassy, or embassy_cross_core for transferable handles, [like this](integrations/embassy/README.md). It also runs in [plain WASM hosts](integrations/wasm/README.md).

Features add execution modes. No runtime is enabled by default. [Other modes and setup fields](aktor/docs/examples.md).

## Docs

[Extra examples](aktor/docs/examples.md)

[Pause and shutdown](aktor/docs/runtime.md)

[Comparisons](aktor/docs/compare-libs.md)

[Docs.rs](https://docs.rs/aktor/latest/aktor/) / [Crates.io](https://crates.io/crates/aktor)
