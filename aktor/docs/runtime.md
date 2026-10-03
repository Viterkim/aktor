# Runtime details

## Requests / replies

send() waits for queue space and gives you a reply to await later. try_send() gives the request back if it can't go straight in. cast() skips the reply, only for functions returning ().

Dropping or timing out a reply leaves queued work running. Drop the last handle and queued calls finish before cleanup. downgrade()/upgrade() gives you a weak handle when you don't want to keep it alive.

## Pause / resume (Tokio)

```rust
database.actor.pause().await?;
database.actor.resume(|| {
    Connection::open_in_memory()
        .map_err(|error| AktorSetupError::new(error.to_string()))
}).await?;
```

pause finishes queued calls and closes the resource, resume opens a new one. replace(setup) does both. Calls made while paused wait for resume, failed setup or cleanup leaves it paused.

shutdown() starts closing immediately, even if you stop awaiting it. Keep your Tokio runtime running until it's done, completion.wait().await gets the result again later.

## Application shutdown

AktorGroup stops its application future and finishes accepted calls, each actor cleans up its own resource, then your last closure gets the reports. If you spawned tasks yourself, they still need stopping in your close flow. `kill.wait_stopping().await` tells your UI or those tasks when we're closing.

Save settings before stopping the group. If you lost a write's reply, it might already have happened, don't blindly retry it.

`AktorGroup::with_grace(duration)` gives the whole shutdown that long, including your last closure. At the deadline unfinished work gets cancelled, browser workers get terminated. Native code can block completely, so a watchdog prints the report and exits if it won't finish. Browser hooks need to yield so the timer can run.

With panic = "abort", a panic in the same process ends it immediately, cleanup can't run then.

## Function arguments

Native queued arguments need Send + 'static, local calls can borrow. Outputs need Send + 'static too. Return owned values.

For nested calls, pass the resource you already have. Using that actor's handle queues work behind yourself and can deadlock.

Leave the generated target inferred with generics, like query::<User, _>(&database, id). Put cfg on the function, attributes on arguments aren't supported.

[Browser setup](../../integrations/worker/README.md)

[Embassy setup](../../integrations/embassy/README.md)
