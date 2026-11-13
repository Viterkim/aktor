# Embassy + alloc

Enable `embassy` and supply an allocator.

```rust
use core::time::Duration;
use er::*;

let started = aktor_start(AktorSetup {
    actors: sensor_setup(spawner),
    shutdown: |report| close_application(report),
    grace: Duration::from_secs(5),
})
.await
.er(())?;

record_many(&started.handles, [4, 5]).await.er(())?;
```

`EmbassyLocal` keeps the actor on your executor, so its state can use `Rc`. Keep polling the driver through shutdown. To call the sensor from another core, enable `embassy_cross_core` and use a critical section that synchronizes the cores. The example's `shared_sensor_setup` shows that setup.

Your board supplies the panic handler and `embassy-time` driver. Run the checks from the repository root.

```sh
bash scripts/check.sh embassy
```

[Example](src/lib.rs)
