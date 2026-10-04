# A bit extra

The [counter example](../examples/counter.rs) owns a number on a thread and prints around each queued call. It writes out the receive loop, dropping the handle lets it finish. spawn_value runs Aktor's loop for you, the group handles shutdown.

Using insert_user from the [README](../../README.md).

## Your own setup / cleanup

These run on the actor's thread, handy if your resource can't be moved there:

```rust
let setup = || {
    let e = |error: rusqlite::Error| AktorSetupError::new(error.to_string());
    let db = Connection::open("users.sqlite").map_err(e)?;
    db.execute("CREATE TABLE IF NOT EXISTS user (name TEXT NOT NULL)", []).map_err(e)?;

    Ok(db)
};
let cleanup = |db: Connection| {
    db.close().map_err(|(_, error)| AktorCleanupError::new(error.to_string()))
};

let database = actors.spawn(ActorArgs::new("users", setup, cleanup)).await?;
```

Give us the text you want in the report, keep whatever else you want in data:

```rust
AktorCleanupError {
    diagnostics: error.er_report_string(),
    data: error,
}
```

That's with er, error.to_string() works too. The group collects the text, the actor's completion keeps your data. AktorSetupError works the same way.

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
let after = async |report: ShutdownReport| {
    if report.failed() {
        eprintln!("{report}");
    }

    Ok::<_, AktorCleanupError>(())
};

let mut actors = AktorGroup::new();
let closing = actors.start_with(after)?;
```

after runs once the actors have cleaned up, closing gives you the report. Save anything still in your UI before stop(), actor cleanup only has its own resource. The [group example](../examples/group.rs) hooks up Ctrl+C and stops its tasks before exiting.

[SQLite example](../examples/sqlite/main.rs)

[Other request options](../examples/guide.rs)

[Pause/resume and shutdown](runtime.md)
