# A bit extra

The [counter example](../examples/counter.rs) owns a number on a thread and prints around each queued call. Its setup gives Aktor the start/end logic and hooks, then shutdown waits for the final count.

TokioTask runs on your Tokio runtime, TokioThread gives blocking work its own thread. With the std_thread feature, StdThread owns a thread without a Tokio runtime.

With bevy, BevyTask(&pool) uses its task pool, BevyLocal(&pool) keeps Rc values on the local executor. Keep that local executor ticking on desktop, both use the browser event loop on the web.

With several setup values:

```rust
let actors = start(aktor_setups! {
    users: users_setup,
    archive: archive_setup,
}).await?;

let id = insert_user(&actors.handles.users, "Katten".into()).await?;
```

Using insert_user from the [README](../../README.md). Keep actors alive while using its handles, dropping it starts group shutdown.

With an existing started group, use `start_in(&group, setup).await?` to get the handles directly. Keep that group alive. It keeps its shutdown listener and grace period, a failed or cancelled startup stops the whole group, including its existing actors.

If startup fails, its .error keeps the typed setup failure and .report has the rollback report, including cleanup failures.

Two connections can have different roles:

```rust
pub mod aktors {
    pub struct Users;
    pub struct Archive;
}
```

Use role: aktors::Users in its setup and #[aktor(role = aktors::Users)] on its functions. AktorNoRole accepts unmarked functions, the name is just what shows up in reports.

## Your own loop

Custom takes a clock, your spawning callback and a loop. Aktor still creates and cleans up the state:

```rust
kind: AktorKind::Custom(
    local::clock::Tokio,
    |future| { tokio::task::spawn_local(future); Ok(()) },
    async |mut runner: AktorRunner<'_, Counter>| {
        while let Some(call) = runner.next().await {
            call.run().await;
        }
        Ok(())
    },
),
```

It runs on that local executor. call.run() applies the hooks and gives the caller its reply, leaving early or dropping a call fails the actor. Use the ordinary modes unless your loop needs to do something extra.

## Browser worker

The parent gives it a program and config:

```rust
let actors = start(AktorWorkerSetup {
    name: AktorName::new("counter"),
    role: AktorNoRole,
    kind: AktorKind::BrowserWebWorker::<Counter>("/counter.js"),
    config: CounterConfig { initial: 0 },
    options: None,
}).await?;
```

Config needs Serialize and Deserialize. The worker builds its AktorClosures with worker::serve_setup, then waits on the returned server. Its start closure creates the resource there, [the working example](../../integrations/worker/src/remote.rs) also has hooks and an interval. Build/options and operation names are checked before sending config. Set the same build on both sides and change it when config or wire fields change.

Cancelling standalone Worker::open before ready terminates it. After ready, dropping the last handle drains accepted work.

## Your own setup / cleanup

These run on the actor's thread, handy if your resource can't be moved there:

```rust
let setup = async || {
    let e = |error: rusqlite::Error| AktorSetupError::new(error.to_string());
    let db = Connection::open("users.sqlite").map_err(e)?;
    db.execute("CREATE TABLE IF NOT EXISTS user (name TEXT NOT NULL)", []).map_err(e)?;

    Ok(db)
};
let cleanup = async |db: Connection| {
    db.close().map_err(|(_, error)| AktorCleanupError::new(error.to_string()))
};

let actors = start(AktorSetup {
    name: AktorName::new("users"),
    role: AktorNoRole,
    kind: AktorKind::TokioThread,
    closures: AktorClosures {
        end: Some(cleanup.into()),
        ..AktorClosures::new(setup)
    },
    options: None,
}).await?;
let database = &actors.handles;
```

With er, use error.er_report_string() for the text you want in the cleanup report. To retain typed data on TokioThread or StdThread, use `AktorKind::TokioThread.with_data::<SetupData, CleanupData>()` and return AktorSetupError<SetupData> / AktorCleanupError<CleanupData>. Setup failures keep their data in the returned error, cleanup data stays available through the actor's completion observer.

## Intervals

With rusqlite's backup feature, put this in closures.intervals:

```rust
vec![AktorInterval {
    every: Duration::from_secs(2 * 60 * 60),
    run: (async |db: &mut Connection| {
        if let Err(error) = db.backup(rusqlite::MAIN_DB, "users.backup.sqlite", None) {
            eprintln!("backup failed: {error}");
        }
    }).into(),
}]
```

The first run waits that duration after setup. Calls and intervals use the state one at a time, a slow callback doesn't build a backlog. Its next wait begins when it finishes, shutdown stops scheduling. On TokioThread, a due callback waits through pause and uses the resumed state.

TokioTask and BevyTask callbacks take AktorTaskState<Connection> here, it dereferences to your connection and keeps the state through awaits. The #[aktor] functions still take their normal references.

## Calling another query

Pass the connection you already have:

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

The whole transaction queues once, the calls inside it run right there.

## Get the reply later

```rust
let reply = insert_user(&database, "Katten".into()).send().await;

// do something else
let id = reply.await?;
```

send waits for queue space. Dropping the reply leaves the insert running.

```rust
let mut reply = insert_user(&database, "Katten".into()).send().await;

if let Some(result) = reply.try_take() {
    show_saved(result?);
}
```

None leaves it available to await later, Some takes the output once.

## Waiting a little less

```rust
let id = insert_user(&database, "Katten".into())
    .timeout(Duration::from_secs(2))
    .await??;
```

The extra result is the timeout you asked for. If it was admitted, the insert keeps running.

## Latest input

For a search box, keep a session in your UI:

```rust
#[aktor]
pub async fn find_users(db: &Connection, query: String) -> Result<Vec<String>> {
    let mut statement = db.prepare("SELECT name FROM user WHERE name LIKE ?")?;
    statement.query_map([format!("%{query}%")], |row| row.get(0))?.collect()
}

let (search, mut results) = find_users(&database, "kat".into()).latest();
search.send("katten".into());

while let Some(rows) = results.next().await {
    show_rows(rows?);
}
```

latest() sends "kat", then your input handler keeps sending. Older results get thrown away, each session keeps its own pending input outside the queue limit. Writes still need their normal replies.

## Closing the application

```rust
let kill = actors.killswitch();
// your Close handler calls kill.stop()
let report = actors.shutdown().await;
if report.failed() {
    eprintln!("{report}");
}
```

Save anything still in your UI before stop(), actor cleanup only has its own resource. Stop the application's pending tasks and await the report before leaving its runtime.

[SQLite example](../examples/sqlite/main.rs)

[Other request options](../examples/guide.rs)

[Pause/resume and shutdown](runtime.md)
