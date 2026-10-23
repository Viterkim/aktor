# Examples

For several actors, give each one a name:

```rust
let actors = aktor_start(AktorSetup {
    actors: aktor_setups! {
        users: users_actor,
        archive: archive_actor,
    },
    shutdown: |report| close_application(report),
    options: Default::default(),
}).await?;

let id = insert_user(&actors.handles.users, "Katten".into()).await?;
```

A tuple works too, you get the handles back in the same order.

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
let reply = insert_user(&database, "Katten".into()).send().await;

// do other work
let id = reply.await?;
```

send waits for queue space. Dropping the reply leaves the operation running. Use cast().await for a function returning () when you don't want its reply.

## Latest input

For a search box:

```rust
let (search, mut results) = find_users(&database, "kat".into()).latest();
search.send("katten".into());

while let Some(rows) = results.next().await {
    show_rows(rows?);
}
```

Only the latest input's result comes back. Use ordinary calls for writes where each input counts.

## Test responses

Skip opening SQLite and give the function a response:

```rust
let database = Preset::<Connection>::new()
    .on(insert_user::handler(async |_| Ok(7)))
    .start(&mut actors)
    .await?;

assert_eq!(insert_user(&database, "Katten".into()).await?, 7);
```

You get a `Mock<Connection>`, so the app can call it as usual. Another .on replaces the response. Missing handlers fail the test. Check the shutdown report too, it catches panics in handlers whose replies you dropped.

[App with test presets](../examples/presets.rs)

## Pause / resume (TokioThread)

```rust
database.actor.pause().await?;
database.actor.resume(open_database).await?;
```

pause finishes queued calls and closes the resource. New calls wait for resume, a failed reopen leaves it paused.

## Setup errors with Er

Keep the internal error report when returning from setup:

```rust
async fn open_database() -> Result<Connection, AktorSetupError> {
    connect().await.er_into(|report| report)
}
```

Startup errors save the rollback report too. Print it through your Er contexts:

```rust
eprintln!("{}", error.er_report());

for diagnostic in error.er_find_all::<AktorDiagnostic>() {
    eprintln!("{}", diagnostic.report);
}
```

[SQLite example](../examples/sqlite/main.rs)

[Request example](../examples/guide.rs)
