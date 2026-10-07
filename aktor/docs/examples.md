# A bit extra

## Setups

The [counter](../../README.md#counter) starts one actor. Give each setup a name to start several together:

```rust
let actors = start(aktor_setups! {
    users: users_setup,
    archive: archive_setup,
}).await?;

let id = insert_user(&actors.handles.users, "Katten".into()).await?;
```

Keep actors alive while using its handles. With an existing started group, `start_in(&group, setup).await?` returns handles instead. Failed or cancelled startup stops that whole group.

TokioTask uses your runtime, StdThread works without Tokio. For local state, use TokioLocal(&local_set), BevyLocal(&pool) or BrowserLocal and keep that executor running. BevyTask(&pool) requires transferable state.

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

Here they run on the owner thread. To keep typed errors, use TokioThread.with_data::<SetupData, CleanupData>() and the matching AktorSetupError<SetupData> / AktorCleanupError<CleanupData>.

The [counter example](../examples/counter.rs) adds hooks. Roles restrict which functions a handle accepts, put role: Users in the setup and #[aktor(role = Users)] on its functions.

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

The whole transaction queues once, insert_user gets the connection directly.

## Get the reply later

```rust
let mut reply = insert_user(&database, "Katten".into()).send().await;

if let Some(result) = reply.try_take() {
    show_saved(result?);
} else {
    let id = reply.await?;
}
```

send waits for queue space, then you can take the output once with try_take or await it. Dropping that reply leaves the operation running.

```rust
let id = insert_user(&database, "Katten".into())
    .timeout(Duration::from_secs(2))
    .await??;
```

The extra result is the timeout, the insert can still finish after you've stopped waiting. Check stored state before retrying a write.

## Latest input

For a search box:

```rust
let (search, mut results) = find_users(&database, "kat".into()).latest();
search.send("katten".into());

while let Some(rows) = results.next().await {
    show_rows(rows?);
}
```

Only the latest input's result comes back. Each session keeps one pending input outside the queue limit, use ordinary replies for writes.

[SQLite](../examples/sqlite/main.rs)

[Request guide](../examples/guide.rs)

[Shutdown](runtime.md)

[Browser workers](../../integrations/worker/README.md)

[Embassy](../../integrations/embassy/README.md)

For your own serving loop, [Custom](../tests/runtime/setup.rs) lets you run accepted calls with call.run().await.
