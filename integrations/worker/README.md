# Browser SQLite

Enable `wasm_browser_workers`. The page uses `AktorWorkerNew` with `AktorKind::BrowserWebWorker("/database-worker.js")` to start the worker and call its functions.

```rust
use er::*;

#[derive(aktor::AktorData)]
struct Record {
    key: String,
    bytes: Vec<u8>,
}

#[derive(Er, aktor::AktorData)]
struct SaveErr {
    key: String,
}

#[aktor::aktor]
async fn save(db: &mut Database, record: Record) -> Result<(), (SaveErr, String)> {
    db.save(record).await.er_split()
}

save(&worker, record).await.er(())?;
```

Open SQLite in the `start` you supply to `worker::serve_setup`. Arguments and results must be owned. `.er_split()` turns the database's `ErResult` into the typed error and report text that the worker can send back to the page. If a type already checks its data in a Serde impl, put `#[aktor(serde)]` on that type alongside `#[derive(AktorData)]`. For a whole function that uses Serde, enable the `serde` feature and put `#[aktor(serde)]` on the function.

To get the worker's own startup error back on the page, use `AktorKind::BrowserWebWorker::<_, OpenErr>::new(url)` with the same Serde error type the worker returns. You'll find it in `AktorInitOutcome::Failed(open_err)` when you match `failure.init`.

When you change a type that crosses the worker boundary, change the build string on both sides. Set `options.transport.build` on the `AktorWorkerNew` and `worker::Options.build` in the worker.

## Optional workers

An unavailable archive worker shouldn't take preferences down with it. Mark the archive worker as optional when you set it up.

```rust
let archive = group.spawn(archive_setup.optional())
    .await
    .er(|| ArchiveErr::Start)?;

save(&archive, record)
    .await
    .er(|| ArchiveErr::Delivery)?
    .er(|| ArchiveErr::Save)?;
```

You get a handle even when the archive couldn't start. If you need to know why, match `failure.init` on the error from `archive.ready().await`. On a delivery failure, `.er()` keeps the `WorkerError` and its report. The last line handles an error from the save itself. The group stays up either way.

## Shutdown

The worker runs its own `end`. With `Confirmed`, the worker finished its work and reported how cleanup went. Cleanup can still fail. With `Unconfirmed`, it might still own its resources. With `Terminated` the worker is gone and won't run any more cleanup.

Calls and `end` run on the other side, so the page can't cancel them. The worker gets both graces in one go. When the time runs out, `stuck: AktorStuck::Leave` lets the worker finish accepted calls and run cleanup. `Kill` terminates the worker and cuts off any `end` still running.

If something on the page still owns the database, put its join in the worker options. Even a forced shutdown waits for it.

```rust
options.before_shutdown = Some(worker::join(join_database()));
```

Returning from `join_database()` tells Aktor the work has stopped, even when closing failed. If it returns an `ErResult<(), CloseErr>`, the group keeps the full report. Dropping a setup before it joins a group still runs the join, but then there's no group to report its error to.

## Build

Install the `wasm32-unknown-unknown` target. Install `wasm-bindgen-cli` at the `wasm-bindgen` version in `integrations/Cargo.lock`. Run the build from the repository root.

```sh
bash integrations/worker/build.sh
```

Serve `integrations/worker/web` from your app.

[Example](src/browser.rs)
