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

If you already have a reply and just want to pick it up when ready:

```rust
let mut reply = insert_user(&database, "Katten".into()).send().await;

if let Some(result) = reply.try_take() {
    show_saved(result?);
}
```

None leaves it available to await later, Some takes the output once.

## Latest input

For a search box, keep a session in your UI:

```rust
let (search, mut results) = find_users(&database, "kat".into()).latest();
search.send("katten".into());

while let Some(rows) = results.next().await {
    show_rows(rows?);
}
```

latest() sends "kat", then you keep sending from your input handler. Results from older inputs get thrown away. Use it for searches, writes still need their normal replies.

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
let kill = actors.killswitch();

// in your Close / Ctrl+C handler
kill.stop();

let report = closing.await;
```

after runs once the actors have cleaned up. Save anything still in your UI before stop(), actor cleanup only has its own resource. The [group example](../examples/group.rs) hooks up Ctrl+C and stops its tasks before exiting.

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
