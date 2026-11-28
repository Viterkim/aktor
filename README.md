# Aktor, not an actor framework

Makes a single owner of a resource (like a SQLite connection or a hardware device) that handles calls one after the other on a thread, async task, browser worker or Embassy.

It basically just swaps out the first argument of your function (`&State` or `&mut State`) for a handle and does the message/queue plumbing for you. You can write normal functions wherever you want (like `users::insert(...)` or `db::get::cat(...)`) with `#[aktor]` above them, call them with a handle and `.await`, and Go to Definition jumps straight to your SQL or whatever you wrote, instead of some giant Request / Response enum or trait boilerplate.

```toml
[dependencies]
aktor = { version = "0.0.13", features = ["tokio"] }
```

[Full examples / usage patterns](aktor/docs/examples.md)

The API can change before 0.1.

## Counter

```rust
use aktor::*;
use core::num::NonZeroUsize;
use std::time::Duration;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;

    *count
}

let counter = aktor_start(AktorSetup {
    stuck: AktorGroupStuck::KeepRunning,
    actors: AktorNew {
        name: "counter".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Ready(async || 0_u32),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(32).unwrap(),
        stuck: AktorStuck::Kill,
    },
    shutdown: |report| {
        if report.failed() {
            eprintln!("{report}");
        }
    },
    grace: Duration::from_secs(5),
});

let count = add(counter.handles(), 5).await?;
```

`counter` owns the actor. Borrow its handle with `counter.handles()`, or call `let (owner, handle) = counter.into_parts();` to move them separately. Dropping the owner stops the actor.

## SQLite

```rust
use aktor::aktor;
use rusqlite::Connection;

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> rusqlite::Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [&name])?;

    Ok(db.last_insert_rowid())
}

let id = insert_user(&database, "Katten".into()).await??;
```

Your `start` closure opens the connection and `end: close` closes it when the actor stops.

Inside another query you already have `&Connection`, so you can pass it straight in instead of the handle (calling your own handle queues behind yourself and deadlocks).

## Loops

Some actors never take a call, they're just a thread around one loop (capture, an audio device, a socket). `AktorLoop` is that, without a fake state and an `end` whose only job is awaiting the loop:

```rust
let (started, capture) = aktor_start(AktorSetup {
    stuck: AktorGroupStuck::KeepRunning,
    actors: AktorLoop {
        name: "capture".into(),
        kind: AktorKind::TokioThread,
        work: async move |mut stopping| {
            let mut device = open_capture_device()?;
            device.record_until(&mut stopping).await
        },
        stuck: AktorStuck::Leave,
    },
    shutdown: |_| {},
    grace: Duration::from_secs(5),
}).into_parts();

let stopped = started.stop().await;
if stopped.failed() {
    log(stopped);
}

let result = capture.result().await;
```

Returning stops the group, including the loop's siblings in a tuple. A loop that can't fail can return `Ok::<_, (Infallible, String)>(())`. With `Err((e, report))` the handle gives you `Err((AktorLoopEnd::Failed(e, id), report))`. The same `id` identifies the loop failure in the shutdown report. A blocking loop can keep a guard from `stopping.on_stop(move || waker.wake())` so the thread closing the actor wakes its OS wait.

## Using 'aktor'

For all patterns/how to use 'aktor' you (or your agent) should read [examples / patterns docs](aktor/docs/examples.md).

Common mistakes are in [good and bad](aktor/docs/good-and-bad.md).

### What `aktor_start` gives you owns the actors

Starting never waits. The handles come back right away and calls just queue until `start` is done, so you can start a dedicated thread from sync code. Start a `TokioTask` inside the Tokio runtime that will run it. Await `.ready()` when you want `start`'s own error:

```rust
let mut store = aktor_start(setup);
store.ready().await?;
```

Neither `.ready()` nor `.stop()` takes it, so it can live in a field: wait for ready once somewhere, stop it in your own cleanup later.

Dropping the started value stops every actor in reverse order. At app exit, `started.stop().await.into_result()` returns an error if a failure stopped the group or nobody received one of its failures. Before reopening a device, use `started.stop_released().await?` to wait until its owner is gone.

Put actors that belong together in one `aktor_start(AktorSetup { stuck: AktorGroupStuck::KeepRunning, actors: (users, archive), .. })`. They stop in reverse order. A required actor failing takes the rest down with it. An optional worker can crash while the others keep running.

### Setup / kinds

Every `AktorNew` field is required, there's no builder quietly filling in `end` or `stuck` for you. A forgotten `end: close_db` looks fine right up until shutdown drops your unflushed state. `end: ()` just drops the state, `capacity` is how many calls can wait in the queue, and `stuck` is what shutdown does with an actor that won't stop in time. `Leave` means leave, `end` and loop tails never get cut at grace, the report still goes out on time. Await `started.stop_released()` to get any failures from their late cleanup. Local, Embassy and custom runner kinds only accept `Kill`, `Leave` on them doesn't compile.

`AktorKind` decides where the actor runs: `TokioThread` for blocking stuff like SQLite, `TokioTask` for async work, `StdThread` without Tokio, `BrowserThread` for shared memory in Wasm, `EmbassyLocal` on embedded. On native `TokioLocal` and `BevyLocal`, the state can be `!Send` and the handles stay on their executor. Call `let (owner, handles) = started.into_parts();` to keep the handles there and move the group owner to another task. The owner carries the startup result, so `start`'s error must be `Send`.

For a browser Web Worker, pass `AktorWorkerNew` to `aktor_start` or `group.spawn`. Its `AktorKind::BrowserWebWorker::<Contract>(url)` shares the config, ready receipt and error types with the worker bootstrap. [Browser worker example](integrations/worker/README.md)

### Errors and shutdown

A query returning `Err` just comes back as a normal `Result` and the actor stays up. A panic shuts down the group and runs cleanup.

`started.stop().await` stops actors in reverse startup order, so cleanup can still call actors that started earlier. For outside loops (like a web server request handler) you can wrap them in `handle.run(...)` so they stop when the group closes.

When the initial group fails to start, every waiter gets the same borrowed `AktorStartError`. Match `Initializer { error, startup }` for your initializer’s own error and its rollback, or `Failed { reason, startup }` for a launch that failed another way. A group that refused the spawn returns `Refused { reason }`.

## Docs

[Examples / Patterns](aktor/docs/examples.md)

[Good and bad](aktor/docs/good-and-bad.md)

[Changelog](changelog.md)

[The same SQLite query with tokio-rusqlite and Aktor](aktor/docs/compare-libs.md)

[Performance](aktor/docs/performance.md)

[Browser SQLite](integrations/worker/README.md)

[Shared memory browser threads](integrations/threads/README.md)

[Remote services](aktor-remote/README.md)

[Embassy](integrations/embassy/README.md)

## Links

[Docs.rs](https://docs.rs/aktor/latest/aktor/)

[Crates.io for the lib](https://crates.io/crates/aktor)

[Crates.io for the macros](https://crates.io/crates/aktor-macros/)

[Crates.io for aktor-remote](https://crates.io/crates/aktor-remote)

[Libs.rs](https://lib.rs/crates/aktor)

[GitHub Repo](https://github.com/Viterkim/aktor)
