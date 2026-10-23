# Browser threads

A worker can share your app's Rust heap. Send a Vec and the allocation moves over, no encoding or copying the bytes.

```rust
#[aktor]
async fn process(state: &mut Processor, bytes: Vec<u8>) -> Vec<u8> {
    state.process(bytes)
}

let bytes = process(&actors.handles, bytes).await;
```

Enable wasm_threads and use AktorKind::BrowserThread("/pkg/app.js") in your setup, pointing to your app's wasm-bindgen module. Arguments, results and setup closures need Send. The state can hold Rc and JavaScript resources created on the worker.

The workers run concurrently and share library globals too. SQLite built with threading disabled breaks here, even with one connection per actor. Use BrowserWebWorker for its own memory.

## Build

Use nightly with rust-src and wasm-bindgen-cli matching Cargo.lock. [build.sh](build.sh) has the atomics flags and rebuilds std.

Serve the app with these headers:

```text
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

The browser needs SharedArrayBuffer and permission to start workers from blob URLs.

Keep resources in actor state so shutdown can close them. wasm-bindgen skips thread local destructors. Stuck synchronous work can outlive the shutdown deadline, and a Wasm trap can leak allocations until the page reloads.
