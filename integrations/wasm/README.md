# Local actors in WASM

The [proof](src/lib.rs) runs the local backend in Wasmi without browser or WASI imports. Values stay in the instance, no serialization needed. The host drives the futures and supplies timers for real use.

```sh
bash scripts/check.sh wasm
```

The runner lives in ../aktor-extras. AKTOR_EXTRAS can point to another copy.
