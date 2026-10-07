# A bit extra

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

Open your database in start and close it in end. For errors:

```rust
Connection::open("users.sqlite")
    .map_err(|error| aktor_err_setup(error.to_string(), error))
```

It keeps the error in data and prints the text you gave it. For cleanup use aktor_err_cleanup, with kind.with_cleanup() to keep the error's type.

Print a startup error with `{error:#}` for the causes and rollback failures, or give it to your error reporter.

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
