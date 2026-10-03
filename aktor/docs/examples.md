# A bit extra

Using insert_user from the [README](../../README.md).

## Get the reply later

```rust
let reply = insert_user(&database, "Katten".into()).send().await;

// do something else
let id = reply.await?;
```

send waits for queue space, then you keep the reply wherever you need it. Dropping it leaves the insert running.

## Waiting a little less

```rust
let id = insert_user(&database, "Katten".into())
    .timeout(Duration::from_secs(2))
    .await??;
```

The extra result is the timeout you asked for. If it was admitted, the insert keeps running.

## Latest input

`find_users(&database, query).await` runs one search. For a search box, make one session and keep its sender in your UI:

```rust
let (search, mut results) = find_users::latest(&database);
search.send("kat".into());
search.send("katten".into());

while let Some(rows) = results.next().await {
    show_rows(rows?);
}
```

Send from your input handler, receive from the task updating your results. Each session keeps its newest input. Older work can finish, its result gets thrown away if you already sent something newer. Use it for searches where skipping a few inputs is fine. For latest, give the return type a name instead of `impl Trait`.

## Closing the application

```rust
let kill = application.killswitch();

// in your Close / Ctrl+C handler
kill.stop();
```

Save anything still in your UI before stop(), actor cleanup only has its own resource. The [group example](../examples/group.rs) hooks up Ctrl+C and collects the reports at the end.

## Setup / cleanup errors

Give us the text you want printed, keep whatever else you want in data:

```rust
AktorCleanupError {
    diagnostics: error.to_string(),
    data: error,
}
```

With er, put `error.er_report_string()` in diagnostics. The group collects the text, the actor's completion keeps your data too. AktorSetupError works the same way.

[SQLite example](../examples/sqlite/main.rs)

[Other request options](../examples/guide.rs)

[Pause/resume and shutdown](runtime.md)
