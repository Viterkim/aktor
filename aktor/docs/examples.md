# A bit extra

## Setups

TokioThread owns a dedicated thread, TokioTask runs on your runtime. StdThread needs the std_thread feature. No runtime is enabled by default.

For local state, use TokioLocal(&local_set), BevyLocal(&pool) or BrowserLocal. Keep the local executor running. BevyTask(&pool) uses transferable state.

```rust
let actors = start(aktor_setups! {
    users: users_setup,
    archive: archive_setup,
}).await?;

let id = insert_user(&actors.handles.users, "Katten".into()).await?;
```

Keep actors alive while using its handles. If you already own a started group, `start_in(&group, setup).await?` returns handles. Failed or cancelled startup stops that whole group. Startup errors keep the original .error and an optional rollback .report.

Roles restrict operations: put role: Users in the setup and #[aktor(role = Users)] on its functions. AktorNoRole accepts unmarked functions.

## Setup / cleanup

```rust
let setup = async || {
    Connection::open("users.sqlite")
        .map_err(|error| AktorSetupError::new(error.to_string()))
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
```

These run on the owner thread. For typed lifecycle data, use TokioThread.with_data::<SetupData, CleanupData>() with AktorSetupError<SetupData> and AktorCleanupError<CleanupData>. StdThread supports it too.

The [counter](../examples/counter.rs) adds before_each / after_each hooks. Put AktorInterval { every, run } in closures.intervals for periodic work. Its next wait begins after the callback finishes, shutdown stops scheduling. Task callbacks use AktorTaskState<S> through awaits.

## Calling another query

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

The transaction queues once. The calls inside it use the connection directly.

## Get the reply later

```rust
let mut reply = insert_user(&database, "Katten".into()).send().await;

if let Some(result) = reply.try_take() {
    show_saved(result?);
} else {
    let id = reply.await?;
}
```

send waits for queue space. try_take takes a ready output once. Dropping an admitted reply leaves the operation running.

```rust
let id = insert_user(&database, "Katten".into())
    .timeout(Duration::from_secs(2))
    .await??;
```

The extra result is the timeout. An admitted insert keeps running, so a timeout doesn't mean the write failed.

## Latest input

For a search box:

```rust
let (search, mut results) = find_users(&database, "kat".into()).latest();
search.send("katten".into());

while let Some(rows) = results.next().await {
    show_rows(rows?);
}
```

Older results get thrown away. Each session keeps one pending input outside the queue limit. Use ordinary replies for writes.

[SQLite](../examples/sqlite/main.rs)

[Request guide](../examples/guide.rs)

[Shutdown](runtime.md)

[Browser workers](../../integrations/worker/README.md)

[Embassy](../../integrations/embassy/README.md)

For your own serving loop, [Custom](../tests/runtime/setup.rs) takes a clock, a spawning callback and a runner. Run each accepted call with call.run().await, Aktor still owns setup and cleanup.
