# While it's running

Calls wait when the queue is full. capacity counts waiting calls on native and local actors, workers include the running call. A dropped reply or timeout leaves admitted work running.

## Pause / resume (TokioThread)

```rust
let reopen = || {
    Connection::open_in_memory()
        .map_err(|error| AktorSetupError::new(error.to_string()))
};

database.actor.pause().await?;
database.actor.resume(reopen).await?;
```

pause finishes queued calls and closes the resource. New calls wait for resume, a failed reopen leaves it paused.

## Shutdown

```rust
let kill = actors.killswitch();
// your Close handler calls kill.stop()

let report = actors.shutdown().await;

if report.failed() {
    eprintln!("{report}");
}
```

Actor failure also wakes kill.wait_stopping(). Stop your caller tasks before awaiting shutdown, their calls can stay pending even past a timeout once the group is closing. With Tokio, abort those tasks or signal them to exit, dropping a JoinHandle leaves its task running.

start owns a runtime for TokioThread groups. With group.start() or run(), keep your executor alive until shutdown finishes.

Local and Embassy drivers need to keep polling through cleanup. Dropping the driver cancels its actors. An operation panic still gets a cleanup attempt when Rust can unwind.

Shutdown gets five seconds by default, then unfinished async work is cancelled and workers are terminated. If native work misses the watchdog's shutdown cutoff, the process will be killed even if a timeout report returns and the work finishes afterward.
