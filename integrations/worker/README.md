# Browser SQLite

The [preferences](src/preferences) queries use #[aktor], running in a worker so SQLite can be busy while the page keeps going. Enable wasm_browser_workers, then register it with your [AktorGroup](../../README.md#opening--starting--spawning):

```rust
use aktor::*;
use worker::Options;
use rusqlite::Connection;

let mut actors = AktorGroup::new();
let kill = actors.killswitch();
let closing = actors.start()?;
let options = Options {
    build: "preferences-v1".into(),
    ..Options::default()
};

let database = actors.worker::<Connection>("preferences", "worker.js", options).await?;

let volume = read(&database, "volume".into()).await?;
```

The group starts its own shutdown task. Put kill.stop() in the UI Close handler and await closing before leaving the page. SQLite runs inside that same worker, open the connection there and start serving:

```rust
let cleanup = async |connection: Connection| {
    connection.close()
        .map_err(|(_, error)| AktorCleanupError::new(error.to_string()))
};
let options = Options {
    build: "preferences-v1".into(),
    ..Options::default()
};

worker::serve_with(connection, cleanup, options)?.wait().await?;
```

Just put #[aktor] above the functions, they're picked up automatically. Arguments and results need Serde to get across, calls inside the worker use the connection directly. Use the same build name on both sides.

[Startup](src/browser.rs) opens SQLite in OPFS (browser storage that survives reloading) and keeps the server alive with wait() while it handles calls. [worker.js](web/worker.js) loads the WASM.

## Try it

Install wasm32-unknown-unknown and wasm-bindgen-cli matching [Cargo.lock](../Cargo.lock), then from the repo root:

```sh
bash integrations/worker/build.sh
python3 -m http.server 8765 --bind 127.0.0.1 --directory integrations/worker/web
```

Open http://127.0.0.1:8765.

## Checks

```sh
npm ci --prefix integrations/worker
npx --prefix integrations/worker playwright install --with-deps chromium
bash scripts/check.sh browser
```

Set AKTOR_BROWSER=firefox or webkit to use another installed Playwright browser. For the same queries natively:

```sh
cargo test --manifest-path integrations/Cargo.toml -p aktor-worker-proof
```
