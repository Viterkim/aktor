# Embassy + alloc

Running `aktor` on embedded (`no_std` + `alloc`) with Embassy timers and executors.

```rust
use core::time::Duration;

let mut started = aktor_start(AktorSetup {
    stuck: AktorGroupStuck::KeepRunning,
    actors: sensor_setup(spawner),
    shutdown: |report| close_application(report),
    grace: Duration::from_secs(5),
});

started.ready().await?;

record_many(started.handles(), [4, 5]).await?;
```

With the `embassy` feature and an allocator, `EmbassyLocal` keeps the actor on your executor so its state can use `Rc` (just keep polling the driver through shutdown). If you want to call the actor from another core, you can enable `embassy_cross_core` with a cross core critical section (see `shared_sensor_setup` in the example).

Your board supplies the panic handler and `embassy-time` driver. At boot, pass a fresh nonzero random 128 bit value to `FailureId::set_producer` before starting actors. That keeps failure IDs distinct across reboots. You can run the checks from the repo root with `bash scripts/check.sh embassy`.

[Example](src/lib.rs)
