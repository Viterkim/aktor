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
