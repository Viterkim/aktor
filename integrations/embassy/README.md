# Embassy + alloc

Enable embassy and supply an allocator. One task runs the owner:

```rust
let (sensor, owner) = embassy::channel::<Sensor, 2, &'static str>()?;
spawner.spawn(sensor_owner(owner)?);
sensor.ready().await?;

let count = record_many(&sensor, [4, 5]).await?;
sensor.shutdown().wait().await?;
```

The 2 is queue capacity. [sensor_owner](src/lib.rs) runs setup and cleanup, record_many calls record with its local sensor. Handles stay on the owner's executor, state and arguments can contain Rc. Outputs still need Send + 'static.

## Try it

The host test uses an Embassy executor:

```sh
cargo test --manifest-path integrations/Cargo.toml -p aktor-embassy-proof
```

For Cortex M0/M4 compilation:

```sh
rustup target add thumbv6m-none-eabi thumbv7em-none-eabihf
rustup +1.89.0 target add thumbv6m-none-eabi thumbv7em-none-eabihf
bash scripts/check.sh embassy
```

On a board you'll need your panic handler and timer driver too. Latest and pause/resume/replace aren't implemented here yet.
