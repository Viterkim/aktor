# Browser SQLite

The [preferences](src/preferences) queries use #[aktor], running in a worker so SQLite can be busy while the page keeps going. Enable the worker feature, then in your page:

```rust
use aktor::*;
use worker::{Options, Worker};
use rusqlite::Connection;

let database = Worker::<Connection>::open(
    "worker.js",
    Options {
        build: "preferences-v1".into(),
        ..Options::default()
    },
)
.await?;

let volume = read(&database, "volume".into()).await?;

database.shutdown().wait().await?;
```

In the worker, list the functions it can receive:

```rust
worker_routes!(
    routes,
    Connection,
    [crate::preferences::read::read, crate::preferences::write::write]
);
```

Those arguments and results need Serde. Calls inside the worker use the connection directly. Use the same build name on both sides.

[Startup](src/browser.rs) opens SQLite in OPFS (browser storage that survives reloading) and passes routes to serve_with. [worker.js](web/worker.js) loads the WASM.

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
