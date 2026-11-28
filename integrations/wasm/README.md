# Local actors in Wasm

Running local `aktor` actors inside Wasmi without any browser or WASI imports, where everything stays in the Wasm instance and the host drives the futures and timers. The host calls `aktor_producer` with a fresh random identity for each instance before driving its actors.

[Example](src/lib.rs)
