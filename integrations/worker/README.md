# Browser SQLite

Enable wasm_browser_workers. The page starts an AktorWorkerNew with BrowserWebWorker("/database-worker.js") and calls its functions:

```rust
#[derive(aktor::AktorData)]
struct Record {
    key: String,
    bytes: Vec<u8>,
}

#[aktor::aktor]
async fn save(db: &mut Database, record: Record) -> Result<(), String> {
    db.save(record).await
}

save(&worker, record).await?;
```

Open SQLite in the worker's worker::serve_setup. Arguments and results must be owned. For Serde types, enable serde and use #[aktor(serde)].

Change worker::Options.build on both sides when you change transported types.

## Optional workers

An unavailable archive worker shouldn't take preferences down with it. Choose that when you create its handle:

```rust
let archive = aktor_start_in(&group, archive_setup.optional()).await?;
save(&archive, record).await??;
```

The handle exists even when the worker couldn't start. Delivery errors come back through ordinary .await. The second ? handles save's own error. Essential workers still fail together.

## Shutdown

An error can arrive before cleanup finishes. worker.shutdown().await returns Retirement:

- Confirmed means cleanup finished, including cleanup that returned an error.
- Unconfirmed means you must keep external owners and their resources. Aktor retains its own pending work until cleanup is confirmed.

Join external work before returning from cleanup or before_shutdown, including when returning an error. Attach before_shutdown through AktorWorkerOptions during construction. Normal and forced shutdown both wait for it.

## Calls during cleanup

A cleanup hook can be waiting for a task that's awaiting a reply from the same worker. Let that task receive its call's outcome before cleanup finishes:

```rust
let reply = operation(&handle, input).admit().await?;
let output = reply.receive_settled().await?;
```

Use this in lifecycle code on native or browser. The outcome does not confirm retirement.

## Build

With wasm32-unknown-unknown and wasm-bindgen-cli matching Cargo.lock:

```sh
bash integrations/worker/build.sh
```

Serve integrations/worker/web from your app.

[Example](src/browser.rs)
