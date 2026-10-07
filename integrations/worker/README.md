# Browser SQLite

With wasm_browser_workers, SQLite runs in a worker and the page calls its functions as usual.

Start it with AktorWorkerNew. The worker opens the database in worker::serve_setup. Both sides use the same build identity, change it when your wire types change.

## Typed data

```rust
#[derive(aktor::AktorData)]
struct Record {
    key: String,
    bytes: Vec<u8>,
}

#[aktor::aktor(data)]
async fn save(db: &mut Database, record: Record) -> Result<(), String> {
    db.save(record).await
}

save(&worker, record).await?;
```

Worker arguments and results have to be owned. #[aktor] uses Serde, #[aktor(data)] uses AktorData as above.

Install wasm32-unknown-unknown and wasm-bindgen-cli matching Cargo.lock, then from the repo root:

```sh
bash integrations/worker/build.sh
cargo run --manifest-path ../aktor-extras/checks/Cargo.toml --bin browser -- "$PWD" serve
```

Open the printed address. Enter stops the server. The browser runner lives in ../aktor-extras, AKTOR_EXTRAS can point to another copy.

[Example](src/browser.rs)
