# Browser threads (shared Wasm memory)

Browser actors running on shared Wasm memory (`SharedArrayBuffer`), so a worker shares your app's Rust heap and you can send a `Vec` straight across without serializing or copying bytes.

```rust
#[aktor]
async fn process(state: &mut Processor, bytes: Vec<u8>) -> Vec<u8> {
    state.process(bytes)
}

let bytes = process(&processor, bytes).await;
```

With the `wasm_threads` feature you can point `AktorKind::BrowserThread("/pkg/app.js")` at your `wasm-bindgen` module. Args, results and setup closures need `Send`, while the actor's own state can still hold `Rc` and JS objects created on that worker.

Because workers share globals too, stuff like single threaded SQLite will break here even with one connection per actor (you can use `BrowserWebWorker` when a worker needs its own memory).

## Build

You'll need the nightly toolchain from [build.sh](build.sh) with `rust-src` (for rebuilding `std` with atomics) and `wasm-bindgen-cli` matching `integrations/Cargo.lock`, and the page needs COOP/COEP headers for `SharedArrayBuffer`:

```text
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

Resources should live inside the actor state so shutdown can close them (`wasm-bindgen` skips thread local destructors, and stuck synchronous code or a Wasm trap can't be cleaned up until reload).
