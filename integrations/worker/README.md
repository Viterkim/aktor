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

The worker builds its resource with worker::serve_setup, [like this](src/remote.rs). Give both sides the same build identity and change it when your wire types change, an incompatible worker is rejected before setup runs.

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

AktorData sends this as compact binary data, including the bytes in bulk, without changing how ordinary Serde sees Record. cache isn't sent, it comes back as Default::default().

Plain #[aktor] uses tagged binary Serde, including for the startup config. With that codec, #[serde(with = "aktor::worker::bytes")] sends byte fields in bulk too. Worker arguments and results must be owned.

Worker capacity includes the running call, latest pending inputs sit outside the ordinary queue budget. If a write times out or loses its reply, check stored state before retrying.

## Try it

Install wasm32-unknown-unknown and wasm-bindgen-cli matching [Cargo.lock](../Cargo.lock), then from the repo root:

```sh
bash integrations/worker/build.sh
cargo run --manifest-path ../aktor-extras/checks/Cargo.toml --bin browser -- "$PWD" serve
```

Open the printed address to try the [SQLite worker](src/browser.rs), it stores data in OPFS. Enter stops the server.

```sh
AKTOR_CHROMIUM=/path/to/chromium bash scripts/check.sh browser
cargo test --manifest-path integrations/Cargo.toml -p aktor-worker-proof
```

The browser runner lives in ../aktor-extras. AKTOR_EXTRAS can point to another copy.
