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

## Saved diagnostics (examples use 'er' for errors)

Setup and cleanup supply the diagnostic text Aktor keeps. Here connect returns an ErResult:

```rust
async fn open_database() -> Result<Connection, AktorSetupError> {
    connect().await.er_into(|report| report)
}
```

With another error library, pass its report text to AktorSetupError::new(text, ()).

A startup error carries the complete report in diagnostics.report, including rollback. Er contexts keep that error available:

```rust
let database = aktor_start(setup).await.er::<OpenErr>(|_| path)?;
```

Further up, collect the saved reports when presenting the failure:

```rust
eprintln!("{}", error.er_report());

for diagnostic in error.er_find_all::<AktorDiagnostic>() {
    eprintln!("\n{}", diagnostic.report);
}
```

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

send waits for queue space, then you can do other work before awaiting the reply. Dropping it leaves the operation running. Use cast().await for a function returning () when you don't want its reply.

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

## Pause / resume (TokioThread)

```rust
database.actor.pause().await?;
database.actor.resume(open_database).await?;
```

pause finishes queued calls and closes the resource. New calls wait for resume, a failed reopen leaves it paused.

[SQLite example](../examples/sqlite/main.rs)

[Request example](../examples/guide.rs)
