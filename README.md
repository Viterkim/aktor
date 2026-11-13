# Aktor, not an actor framework

Write normal functions and call them with a handle and `.await`. Go to Definition takes you to the actual SQL or whatever you wrote. An actor runs one call at a time and can live on a native thread or in a browser worker.

```toml
[dependencies]
aktor = { version = "0.0.13", features = ["tokio"] }
```

The API can change before 0.1.

## Counter

```rust
use aktor::*;
use std::time::Duration;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;

    *count
}

let started = aktor_start(AktorSetup {
    actors: AktorNew {
        name: "counter".into(),
        kind: AktorKind::TokioThread,
        start: async || aktor_ready(0_u32),
        end: (),
        hooks: Default::default(),
        capacity: 32,
        stuck: AktorStuck::Kill,
    },
    shutdown: |report| {
        if report.failed() {
            eprintln!("{report}");
        }
    },
    grace: Duration::from_secs(5),
})
.await?;

let count = add(&started.handles, 5).await;
```

A failed start gives you `(AktorStartError<E>, String)`. Match `AktorInitOutcome::Failed(error)` in the startup error's `init` field to handle an error from your initializer. The report includes how rollback went for actors that already started. In an Er app, `.await.er(())?` keeps both the typed error and the report.

Keep the group where you keep the handles. Returning just `started.handles` drops the group and starts shutting down the actor you just opened.

```rust
let store = Store {
    counter: started.handles,
    group: started.group,
};
```

A query returning `Err` is just a query returning `Err`. If the database actor panics, the group shuts down and runs cleanup. Wrap outside work like a server's request loop in the handle's `run` so it stops when shutdown starts. [Code that lives longer than the group](aktor/docs/good-and-bad.md#code-that-lives-longer-than-the-group).

Use `started.shutdown().await` to close the group and get its report. Actors close in reverse order so an actor can still call the ones that started before it during cleanup. [Starting more actors and shutting down](aktor/docs/examples.md).

## SQLite

Here we use Er 0.9 so a failed insert tells us which name we tried to add.

```rust
use aktor::aktor;
use er::*;
use rusqlite::Connection;

#[derive(Er)]
pub struct InsertUserErr {
    pub name: String,
}

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> ErResult<i64, InsertUserErr> {
    db.execute("INSERT INTO user (name) VALUES (?)", [&name])
        .er(&name)?;

    Ok(db.last_insert_rowid())
}

let id = insert_user(&database, "Katten".into()).await?;
```

Open the connection in `start` and close it in `end`. Use `TokioThread` on native. In a browser worker the query still takes the connection, and its reply needs a type that can cross the worker boundary. [Browser SQLite](integrations/worker/README.md).

Inside another query you already have the connection, so pass it in. A call using the handle would queue behind the one you're still in.

## More

[Examples](aktor/docs/examples.md)

[Changelog](changelog.md)

[Good and bad](aktor/docs/good-and-bad.md)

[The same SQLite query with tokio-rusqlite and Aktor](aktor/docs/compare-libs.md)

[Performance](aktor/docs/performance.md)

[Shared memory browser threads](integrations/threads/README.md)

[Remote services](aktor-remote/README.md)

[Embassy](integrations/embassy/README.md)

## Links

[Docs.rs](https://docs.rs/aktor/latest/aktor/)

[Crates.io for the lib](https://crates.io/crates/aktor)

[Crates.io for the macros](https://crates.io/crates/aktor-macros/)
