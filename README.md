# Aktor, not an actor framework

Write normal functions, call them with a handle and .await. Go to definition takes you to the actual SQL or whatever you wrote. The actor runs one call at a time, on a native thread or a browser worker.

```toml
[dependencies]
aktor = { version = "0.0.9", features = ["tokio"] }
```

The API can change before 0.1.

## Counter

```rust
use aktor::*;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;
    *count
}

let actors = aktor_start(AktorSetup {
    actors: AktorNew {
        name: AktorName::new("counter"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            start: async || Ok(0_u32),
            end: (),
            intervals: vec![],
            before_each: None,
            after_each: None,
        },
        options: Default::default(),
    },
    shutdown: |report| {
        if report.failed() {
            eprintln!("{report}");
        }
    },
    options: Default::default(),
}).await?;

let count = add(&actors.handles, 5).await;
```

Keep actors alive, dropping it starts shutdown. Await actors.shutdown() to finish cleanup and get the report.

An actor failure shuts down the group and runs cleanup. A query returning Err just gives you the error. A native actor stuck past the shutdown deadline takes the process down with it.

## SQLite, including the browser

```rust
#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> Result<i64, String> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])
        .map_err(|error| error.to_string())?;

    Ok(db.last_insert_rowid())
}

let id = insert_user(&actors.handles, "Katten".into()).await?;
```

Open the connection in start and close it in end. Use TokioThread on native, or [BrowserWebWorker](integrations/worker/README.md) to open it in a browser worker. Same query, same call.

Inside another query, pass the connection directly, passing its handle queues behind yourself.

## More

[Examples](aktor/docs/examples.md)

[Shared memory browser threads](integrations/threads/README.md)

[Remote services](aktor-remote/README.md)

[Embassy](integrations/embassy/README.md)

## Links

[Docs.rs](https://docs.rs/aktor/latest/aktor/)

[Crates.io for the lib](https://crates.io/crates/aktor)

[Crates.io for the macros](https://crates.io/crates/aktor-macros/)
