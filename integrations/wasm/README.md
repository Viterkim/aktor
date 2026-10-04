# Local actors in WASM

This uses the same local backend as the Embassy example. It runs as a plain wasm32-unknown-unknown module in Node and Wasmtime, with no browser, Web Workers, WASI or host imports.

The host still has to drive the futures. [The proof](src/lib.rs) does that with a little polling loop, everything stays in this instance so it doesn't need Serde. Rust supplies the allocator here.

## Try it

```sh
python3 -m venv target/wasm-host-env
target/wasm-host-env/bin/python -m pip install -r integrations/wasm/requirements.txt
bash scripts/check.sh wasm
```

prepare.sh runs this too, so set up that Python env first. AKTOR_WASMTIME_PYTHON can point to an existing one.

Real timers need an embassy-time driver connected to your host, the proof uses a little test clock. The host also has to stop it if code stops yielding.
