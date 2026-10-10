# Aktor, not an actor framework

Write normal functions, call them with a handle and .await. Go to definition takes you to the actual SQL or whatever you wrote. One owner runs the calls one after the other, and the same functions work on a native thread or a browser worker.

```toml
[dependencies]
aktor = { version = "0.0.6", features = ["tokio"] }
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

TokioThread owns the counter on its own thread. Calls wait for queue space. Keep actors alive while using its handles, dropping it starts shutdown.

## A Tokio app

Aktor does the channel plumbing. Open your resources in start and close them in end. If an actor fails, the group shuts down and runs cleanup, then your shutdown closure can tell the app to close. A query returning Err still returns it to the caller.

With Tokio's signal feature enabled, your main can do this around a server returning io::Result<()>:

```rust
let kill = actors.killswitch();
let result = tokio::select! {
    result = serve_http(&actors.handles) => result,
    result = tokio::signal::ctrl_c() => result,
    _ = kill.wait_stopping() => Ok(()),
};

let report = actors.shutdown().await;
result?;

if report.failed() {
    return Err(report.into());
}
```

So a server error also goes through shutdown before leaving main. The select drops the server future when stopping begins, releasing what it held while actor cleanup runs. Native groups keep their shutdown runtime alive until cleanup finishes.

## Multiplatform SQLite example (Tokio and web worker)

Write the query once and call it the same way on either platform.

```rust
#[aktor(data)]
pub async fn insert_user(db: &Connection, name: String) -> Result<i64, String> {
    db.execute("INSERT INTO user (name) VALUES (?)", [name])
        .map_err(|error| error.to_string())?;

    Ok(db.last_insert_rowid())
}

#[cfg(not(target_family = "wasm"))]
let database_actor = AktorNew {
    name: AktorName::new("sqlite"),
    role: AktorNoRole,
    kind: AktorKind::TokioThread,
    closures: sqlite_closures(),
    options: Default::default(),
};

#[cfg(target_family = "wasm")]
let database_actor = AktorWorkerNew::<Connection, _> {
    name: AktorName::new("sqlite"),
    role: AktorNoRole,
    kind: AktorKind::BrowserWebWorker("/database-worker.js"),
    config: (),
    options: Default::default(),
};

let actors = aktor_start(AktorSetup {
    actors: database_actor,
    shutdown: |_report| { /* tell your app to close */ },
    options: Default::default(),
}).await?;

let id = insert_user(&actors.handles, "Katten".into()).await?;
```

sqlite_closures opens the connection in start and closes it in end. The worker entry point serves those closures:

```rust
worker::serve_setup(AktorNoRole, Default::default(), |()| sqlite_closures())
    .await?.wait().await?;
```

#[aktor(data)] uses AktorData for worker arguments and results, including the query's error. Derive AktorData on your own transported types. The connection stays with its owner. Inside another query, pass that connection directly, passing its handle queues behind yourself.

## More

[Examples](aktor/docs/examples.md)

[Browser workers](integrations/worker/README.md)

[Embassy](integrations/embassy/README.md)

## Links

[Docs.rs](https://docs.rs/aktor/latest/aktor/)

[Crates.io for the lib](https://crates.io/crates/aktor)

[Crates.io for the macros](https://crates.io/crates/aktor-macros/)
