# Local actors in WASM

The example runs local actors in Wasmi, with no browser or WASI imports. Values stay in the instance. Your host drives the futures and supplies any timers you need.

```sh
bash scripts/check.sh wasm
```

The runner lives in ../aktor-extras. AKTOR_EXTRAS can point to another copy.

[Example](src/lib.rs)
