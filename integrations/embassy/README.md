# Embassy + alloc

Enable embassy and supply an allocator:

```rust
let actors = aktor_start(AktorSetup {
    actors: sensor_setup(spawner),
    shutdown: |report| close_application(report),
    options: Default::default(),
}).await?;
let count = record_many(&actors.handles, [4, 5]).await?;
```

EmbassyLocal keeps everything on your executor, Rc is fine. Keep polling the driver through shutdown. Another core needs embassy_cross_core, shared_sensor_setup and a critical-section implementation that synchronizes the cores.

Your board supplies the panic handler and embassy-time driver. Run the checks with:

```sh
bash scripts/check.sh embassy
```

[Example](src/lib.rs)
