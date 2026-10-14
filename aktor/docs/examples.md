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

Setup returns `Result<State, AktorSetupError>`, cleanup returns `Result<(), AktorCleanupError>`. Put the diagnostic you want kept in the report:

```rust
start: async || open_database().er_into(|report| report),
end: async |db| close_database(db).er_into(|report| report),
```

Here opening and closing return `ErResult`, and Aktor saves their reports automatically. With another error library, give `AktorSetupError::new(text, ())` or `AktorCleanupError::new(text, ())` its diagnostic text. Your error library and its version are yours to choose. Infallible cleanup can return (), and end: () leaves it out.

## Startup and rollback

`users` starts, then opening `archive.sqlite` fails. Aktor rolls back `users`, and closing its database fails too. The returned `AktorStartupError` prints `actor startup failed` normally. Both failures are still there.

Print the ordinary error first, then the Aktor details separately:

```rust
eprintln!("{err}");

eprintln!("Aktor startup and rollback:\n{err:#}");
```

`{:#}` includes the startup diagnostic and any rollback failures. Display and Debug stay short on purpose. Putting the whole saved report there would stuff another giant report inside one entry of your error library's report. Keep them apart and print both.

`AktorStartupError::source()` returning `None` is intentional too. For an error panel, the startup text is in `err.diagnostics` and the optional rollback report is in `err.report`. A `ShutdownReport` already prints its full contents with `{report}`.

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

## Error reporting libraries (Like 'er')

For an `ErResult`, use `.er_into(|report| report)` to save the report automatically. Aktor’s public errors use ordinary field conversions, so they don’t need your Er version. An explicit payload uses `.er_into(|report| (report, data))`.
