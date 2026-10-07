# Embassy + alloc

Enable embassy and supply an allocator. The sensor setup puts its driver on your executor:

```rust
let actors = aktor_start(AktorSetup {
    actors: sensor_setup(spawner),
    shutdown: |report| close_application(report),
    options: Default::default(),
}).await?;
let count = record_many(&actors.handles, [4, 5]).await?;
```

With EmbassyLocal everything stays on that executor, Rc is fine. Keep its driver polling through shutdown. Calling from another core needs embassy_cross_core and shared_sensor_setup, with a critical-section implementation that synchronizes the cores.

Your board supplies its panic handler, allocator and embassy-time timer driver. To run the checks:

```sh
bash scripts/check.sh embassy
```

[Example](src/lib.rs)
