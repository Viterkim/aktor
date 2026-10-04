# While it's running

Calls wait when the queue is full. Once a call is queued, dropping its reply or timing out leaves it running. Losing a write's reply doesn't mean it failed, so don't blindly retry it.

## Pause / resume (Tokio)

```rust
let reopen = || {
    Connection::open_in_memory()
        .map_err(|error| AktorSetupError::new(error.to_string()))
};

database.actor.pause().await?;
database.actor.resume(reopen).await?;
```

pause finishes queued calls and closes the resource. Calls wait until resume opens it again. If reopening fails, they keep waiting for a later resume.

## Application shutdown

Put kill.stop() in your Close handler. Actor failure also wakes kill.wait_stopping(), forward that into the same handler:

```rust
let stopping = kill.clone();
spawn(async move {
    stopping.wait_stopping().await;
    events.send(AppEvent::Close).await;
});
```

Your handler stops your application's tasks, then awaits closing before leaving the runtime. A task waiting on a dead actor stays pending until you drop it. Dropping a Tokio JoinHandle leaves its task running.

Save settings still in your UI before stopping the group. Actor cleanup deals with its own resource, start_with(after) runs your final closure afterward.

Shutdown gets five seconds by default. Unfinished work is cancelled at the deadline, browser workers are terminated. If native code won't stop, the watchdog prints the report and exits the process. Browser code needs to yield so its timer can run.
