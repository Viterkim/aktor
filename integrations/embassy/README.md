# Embassy + alloc

Enable embassy and supply an allocator. The [sensor setup](src/lib.rs) puts its driver on your executor:

```rust
let actors = start(sensor_setup(spawner)).await?;
let count = record_many(&actors.handles, [4, 5]).await?;
let report = actors.shutdown().await;
```

With EmbassyLocal everything stays on that executor, Rc is fine. Keep its driver polling through shutdown.

To call from another core, enable embassy_cross_core and use [shared_sensor_setup](src/lib.rs). Its handles and messages are Send, the state stays on the owner. Your critical-section implementation must synchronize those cores.

Supply embassy-time's timer queue too. pause/resume/replace aren't supported here.

## Try it

Host tests and Cortex M0/M4 compilation:

```sh
cargo test --manifest-path integrations/Cargo.toml -p aktor-embassy-proof
rustup target add thumbv6m-none-eabi thumbv7em-none-eabihf
rustup +1.89.0 target add thumbv6m-none-eabi thumbv7em-none-eabihf
bash scripts/check.sh embassy
```

A board also needs its panic handler and timer driver. [Plain WASM](../wasm/README.md) uses the local backend too.
