# Aktor, not an actor framework

Write some sql in a normal function, pass your handle and `.await` it. One owner runs the calls one after the other, 'go to definition' takes you to the function you wrote.

```toml
[dependencies]
aktor = { path = "aktor", features = ["tokio"] }
```

This checkout has the next setup API. [0.0.3's docs](https://docs.rs/aktor/0.0.3/aktor/) cover the published version. Anything up to 0.1 will not have a stable api.

## Counter

```rust
use aktor::*;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;
    *count
}

let actors = start(AktorSetup {
    name: AktorName::new("counter"),
    role: AktorNoRole,
    kind: AktorKind::TokioThread,
    closures: AktorClosures::new(async || Ok(0_u32)),
    options: None,
}).await?;

let count = add(&actors.handles, 5).await;
let report = actors.shutdown().await;
```

TokioThread owns the state on its own thread. The defaults give you a queue of 32 and five seconds to shut down. Keep actors alive, dropping it starts group shutdown.

## SQLite

```rust
#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> rusqlite::Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])?;

    Ok(db.last_insert_rowid())
}

let id = insert_user(&database, "Katten".into()).await?;
```

Create the connection in your start closure, [like this](aktor/examples/sqlite/main.rs). Your function's errors come back as usual. Inside another query, pass the connection you already have. Passing its handle queues behind yourself.

## Docs

[Setups and request options](aktor/docs/examples.md)

[Pause and shutdown](aktor/docs/runtime.md)

[Browser workers](integrations/worker/README.md)

[Embassy](integrations/embassy/README.md)

[Plain WASM](integrations/wasm/README.md)

[Comparisons](aktor/docs/compare-libs.md)

[Performance](aktor/docs/performance.md)

[Docs.rs](https://docs.rs/aktor/latest/aktor/)

[Crates.io](https://crates.io/crates/aktor)
