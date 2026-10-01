# A bit extra

Using insert_user from the [README](../../README.md).

## Get the reply later

```rust
let reply = insert_user::request(&database, "Katten".into()).send().await;

// do something else
let id = reply.await?;
```

send waits for queue space, then you keep the reply wherever you need it. Dropping it leaves the insert running.

## Keep the newest queued search

```rust
let names = find_users::request(&database, "kat".into())
    .latest("user search")
    .checked()
    .await??;
```

A newer queued call to find_users with that key replaces the old one, which gets CallError::Superseded. A search already running finishes. Give different panels different keys. Works with tokio and browser workers.

[SQLite example](../examples/sqlite/main.rs)

[Other request options](../examples/guide.rs)

[Pause/resume and checked()](runtime.md)
