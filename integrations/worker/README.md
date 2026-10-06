# Browser SQLite

Enable wasm_browser_workers. The [preferences queries](src/preferences) run in a worker while the page keeps going.

```rust
let actors = start(AktorWorkerSetup {
    name: AktorName::new("counter"),
    role: AktorNoRole,
    kind: AktorKind::BrowserWebWorker::<Counter>("/counter.js"),
    config: CounterConfig { initial: 0 },
    options: None,
}).await?;
```

The worker builds its closures with worker::serve_setup and awaits server.wait(), [like this](src/remote.rs). Config uses Serde. Set the same build identity on both sides and change it when your wire types change, compatibility is checked before setup runs.

## Typed data

```rust
#[derive(aktor::AktorData)]
struct Record {
    key: String,
    bytes: Vec<u8>,
    #[aktor(skip)]
    cache: String,
}

#[aktor::aktor(data)]
async fn save(db: &mut Database, record: Record) -> Result<(), String> {
    db.save(record).await
}

save(&worker, record).await?;
```

AktorData uses compact binary data and sends Vec<u8> in bulk. It leaves ordinary Serde alone. Skipped fields come back through Default, handy for local caches.

Plain #[aktor] uses tagged binary Serde. For bulk bytes there, use #[serde(with = "aktor::worker::bytes")]. Worker arguments and results must be owned. Both codecs are binary.

capacity counts outstanding ordinary calls, including the running call. An oversized message takes the whole byte budget and runs alone. Latest pending inputs sit outside that budget. A timeout or lost reply can leave a write unconfirmed, reconcile with stored state before retrying.

## Try it

Install wasm32-unknown-unknown and wasm-bindgen-cli matching [Cargo.lock](../Cargo.lock), then from the repo root:

```sh
bash integrations/worker/build.sh
cargo run --manifest-path ../aktor-extras/checks/Cargo.toml --bin browser -- "$PWD" serve
```

Open the printed address, Enter stops the server. [The SQLite worker](src/browser.rs) uses OPFS for persistent browser storage.

```sh
AKTOR_CHROMIUM=/path/to/chromium bash scripts/check.sh browser
cargo test --manifest-path integrations/Cargo.toml -p aktor-worker-proof
```

The browser runner lives in ../aktor-extras. AKTOR_EXTRAS can point to another copy.
