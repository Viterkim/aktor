# While it's running

Native and local capacity is the waiting queue, one call can also be running. Browser workers count that running call too. Calls wait when full, latest sessions keep one pending input outside those limits. Once a call is queued, dropping its reply or timing out leaves it running. Losing a write's reply doesn't mean it failed, so don't blindly retry it. Browser input encoding failures are NotAdmitted, answer decoding failures are OutcomeUnknown.

Pending calls can outlive their timeout once the group is stopping, drop them with the rest of your application's tasks. Closing admission during group shutdown rejects unqueued work without adding an actor failure, an admitted call losing its output still reports failure.

## Pause / resume (TokioThread)

```rust
let reopen = || {
    Connection::open_in_memory()
        .map_err(|error| AktorSetupError::new(error.to_string()))
};

database.actor.pause().await?;
database.actor.resume(reopen).await?;
```

pause finishes ordinary queued calls and closes the resource, latest sessions keep their pending input for resume. Calls wait until resume opens it again. If reopening fails, they keep waiting for a later resume.

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

With Tokio or browser run(), dropping that future starts shutdown too. Actor cleanup keeps going, your final closure is cancelled and the report says so. Keep the runtime alive and await the completion. Local and Embassy need their driver to keep polling, dropping listen() or run() cancels the actors and publishes a failed report. Dropping a completion observer is fine.

Save settings still in your UI before stopping the group. Actor cleanup deals with its own resource, start_with(after) runs your final closure afterward.

With Tokio or StdThread and unwinding enabled, a local operation or hook panic ends serving and still attempts the actor's end closure within the shutdown budget. The original panic stays primary, cleanup failures are retained too. Aborting or dropping the driver can't promise async cleanup.

Shutdown gets five seconds by default. At the deadline, pending actor work and your final closure are cancelled, browser workers are terminated. Native shutdown reserves a short settlement window before that deadline. Missing its cutoff commits the watchdog to killing the process (SIGKILL on Unix, abort elsewhere), even if a timeout report returns and the remaining work finishes afterward. Printing the report is best effort. Browser code needs to yield so its timer can run. Stopping before start completes an empty group, it can't be started afterward.

Your own spawn_blocking work can outlive the final closure, runtime.shutdown_background() lets you leave after the report.
