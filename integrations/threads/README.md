# Browser threads

A worker can share your app's Rust heap. Send it a `Vec` and it takes over the same allocation without encoding or copying the bytes.

```rust
#[aktor]
async fn process(state: &mut Processor, bytes: Vec<u8>) -> Vec<u8> {
    state.process(bytes)
}

let bytes = process(&processor, bytes).await;
```

Enable `wasm_threads`. Use `AktorKind::BrowserThread("/pkg/app.js")` in your setup and point it to your app's `wasm-bindgen` module. Arguments, results and setup closures need `Send`. The state can hold `Rc` and JavaScript resources created on the worker.

The workers run concurrently and share library globals too. SQLite built with threading disabled breaks here, even with one connection per actor. Use `BrowserWebWorker` to give the worker its own memory.

## Build

Install the nightly toolchain named in `build.sh` with `rust-src`. The script sets the atomics flags and rebuilds `std`. Install `wasm-bindgen-cli` at the `wasm-bindgen` version in `integrations/Cargo.lock`. [Build script](build.sh)

Serve the app with these headers.

```text
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

The browser needs `SharedArrayBuffer` and permission to start workers from blob URLs.

Keep resources in actor state so shutdown can close them. `wasm-bindgen` skips thread local destructors. Stuck synchronous work can outlive the shutdown deadline. A Wasm trap can leak allocations until the page reloads.
