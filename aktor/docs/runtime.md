# Closing the app

Have your Close or Ctrl C handler call actors.killswitch().stop(). An actor failure starts shutdown too, while an ordinary error returned by your function goes back to its caller.

The shutdown closure runs after actor cleanup, so the app can finish closing when it receives the report:

```rust
let actors = aktor_start(AktorSetup {
    actors: (database_actor, cache_actor),
    shutdown: |report| {
        if report.failed() {
            eprintln!("{report}");
        }

        request_app_close();
    },
    options: Default::default(),
}).await?;
```

report.startup marks startup rollback, so the callback can leave retrying to the code starting the actors.

Jobs started with actors.group.spawn_task are cancelled when stopping begins. Other application work holding resources that cleanup needs should release them when actors.killswitch().wait_stopping() completes.

To request shutdown and wait for it yourself, use actors.shutdown().await. Don't wait for this group inside its own shutdown callback, completion waits for that callback to return.

TokioTask runs on your Tokio runtime, so keep that runtime alive and driven until actors.shutdown().await finishes. Dropping it during cleanup cancels the actor and produces a failed report. Supplied LocalSet, TaskPool, Custom and Embassy executors need to keep driving their actors through cleanup too. TokioThread and StdThread own their execution and can finish after your calling runtime exits.

Shutdown gets five seconds by default, change options.shutdown_grace if it needs longer. A native actor that still won't stop can cause Aktor to terminate the process.
