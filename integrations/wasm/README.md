# Local actors in Wasm

The example runs local actors in Wasmi without browser or WASI imports. Values stay in the Wasm instance. The host drives the futures and supplies any timers you need.

[Example](src/lib.rs)
