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

With wasm32-unknown-unknown and wasm-bindgen-cli matching Cargo.lock:

```sh
bash integrations/worker/build.sh
```

Serve integrations/worker/web from your app.

[Example](src/browser.rs)

An unavailable archive worker shouldn't take preferences down with it. Choose that when you create its handle:

```rust
let archive = aktor_start_in(&group, archive_setup.optional()).await?;
save(&archive, record).await??;
```

The handle exists even when the worker couldn't start. Delivery errors come back through ordinary .await. The second ? handles save's own error. Essential workers still fail together.

An error reply can arrive while work is still alive. worker.shutdown().await returns Retirement::Confirmed once cleanup has finished, including cleanup that returned an error. With Retirement::Unconfirmed, Aktor keeps its uncertain work retained, and you must keep the external owner too.

Cleanup must join its external work before returning. Put joins outside that hook in AktorWorkerOptions.before_shutdown during construction. Normal and forced shutdown both wait for it.

Cleanup can be waiting for the worker that's awaiting a reply. That worker needs the call's outcome before cleanup can finish:

```rust
let reply = operation(&handle, input).admit().await?;
let output = reply.receive_settled().await?;
```

Use this in lifecycle code on native or browser. The outcome does not confirm retirement.
