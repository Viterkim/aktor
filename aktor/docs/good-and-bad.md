# Good and bad

Mistakes that compile fine and only bite you at runtime.

## Keep the owner when you move a handle

```rust
let (started, database) = aktor_start(setup).into_parts();
let app = App { started, database };
```

If you only need a handle for a call, borrow it with `started.handles()`. When two actors keep each other's handles they still stop as soon as you drop the owner.

## Don't call your own handle inside a query

```rust
// Bad, queues behind yourself and deadlocks
#[aktor]
pub async fn insert_two(db: &Connection, handle: Aktor<Connection>) -> Result<(), AppError> {
    insert_user(&handle, "Katten".into()).await??;
    Ok(())
}

// Good, pass the connection you already have
#[aktor]
pub async fn insert_two(db: &Connection) -> rusqlite::Result<()> {
    insert_user(db, "Katten".into()).await?;
    Ok(())
}
```

Every `#[aktor]` function takes a handle OR `&State` / `&mut State`. Inside the actor you can always pass the state directly.

## Don't call your own actor from end

The mailbox closes before `end` runs, so a call into your own actor returns `NotAdmitted`. Pass the state directly to finish the work inside `end`.

Your own cleanup should happen before calling another actor from `end`. If actor B needs actor A during shutdown, you can start B after A so reverse shutdown order closes B while A is still up.

## Don't put endless loops inside a `#[aktor]` call

One call runs at a time, so a server loop or audio stream inside a `#[aktor]` call blocks every other call on that actor. You can use `aktor_spawn(work, AktorJob::FinishBeforeEnd)?` with `aktor_stopping()` instead so calls can still run.

## Wrap outside loops in handle.run(...)

A request handler or UI task can outlive a group and call into it after it closes, so you can wrap it once at the top with `handle.run(...)`:

```rust
let Some(response) = database.run(handle(&database, request)).await else {
    return unavailable();
};
```

When shutdown starts, `.run()` drops the handler and returns `None`, so you handle it in one spot instead of everywhere inside `handle`.

## Don't check "is the actor dead?" on every call

If a thread or required worker dies, the whole group shuts down and runs cleanup anyway, so checking for dead actors on every call just adds useless boilerplate.

```rust
let id = insert_user(&database, name).await??;
```

The first `?` handles delivery. The second handles the database error. A required actor's crash stops its group, while an optional worker can fail and the rest of the group can keep running. If you receive an `AktorDeliveryError` from that crash, its `failure_id` identifies the same row in the shutdown report. A delivered optional failure alone lets `report.into_result()` succeed. If the worker crashes while idle and nobody reads its completion, shutdown still reports the error.
