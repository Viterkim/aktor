# While it's running

Calls wait when the queue is full. Native and local capacity counts waiting calls, browser workers count the running call too. Dropping an admitted reply or timing out leaves the operation running, so don't blindly retry writes.

## Pause / resume (TokioThread)

```rust
let reopen = || {
    Connection::open_in_memory()
        .map_err(|error| AktorSetupError::new(error.to_string()))
};

database.actor.pause().await?;
database.actor.resume(reopen).await?;
```

pause drains queued calls and closes the resource. Calls wait for resume, latest sessions keep their pending input. A failed reopen leaves it paused.

## Shutdown

```rust
let kill = actors.killswitch();
// your Close handler calls kill.stop()
let report = actors.shutdown().await;
if report.failed() {
    eprintln!("{report}");
}
```

Actor failure also wakes kill.wait_stopping(). Stop your application's caller tasks and await shutdown while the runtime is alive. Pending calls can stay parked past their timeout during shutdown. Dropping a Tokio JoinHandle leaves its task running.

Prepared TokioThread groups keep supervision and cleanup on their own runtime, with one sleeping management thread per group. They can finish after the caller's runtime closes. Groups started with group.start() or run() use your executor, keep it alive until shutdown finishes.

Local and Embassy drivers must keep polling for cleanup. Dropping their driver cancels the actors, dropping a completion observer is fine. With unwinding enabled, an operation panic still attempts cleanup and retains its failures.

The default budget is five seconds. Unfinished cooperative work is cancelled and browser workers are terminated. Native work that misses the settlement cutoff commits the watchdog to killing the process, even if a timeout report returns and work finishes afterward.
