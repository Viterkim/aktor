# Good and bad

## The actor being gone isn't yours to handle

A failed insert is worth handling at the call site. When a dedicated thread or a required worker dies, the whole group shuts down and runs cleanup, so checking for that on every call just gives you the same decision to make all over the app.

```rust
let id = insert_user(&database, name).await?;
```

The `?` is for the error from your query.

## Code that lives longer than the group

A server with one group per tenant can still be handling a request when that tenant's group closes. A call into a closed group can wait forever. Run the handler with the tenant's handle once, at the top.

```rust
let Some(response) = database.run(handle(&database, request)).await else {
    return unavailable();
};
```

If the handler is still running when the group starts closing, `.run()` drops it and returns `None`. Everything inside `handle` just calls its functions, and this one place decides what the client hears when the tenant goes down.

When cleanup needs to know whether a write ran on a native dedicated thread or a `BrowserWebWorker`, use `write(&database, value).admit().await` and keep the reply with the job that owns the write. That job can call `reply.receive_settled().await` to get the outcome while the group is closing. A lost reply can still mean the write happened.
