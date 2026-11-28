# Examples / Patterns

All native examples use `use aktor::*;` with the `tokio` feature.

## Basic actor

With `#[aktor]` on a normal `async fn` taking `&State` or `&mut State`, you describe the actor with an `AktorNew` and start it with `aktor_start`. Every field is written out, so the `end` that closes your database and the queue size are always a choice someone made:

```rust
use aktor::*;
use core::num::NonZeroUsize;
use rusqlite::Connection;
use std::time::Duration;

#[aktor]
pub async fn insert_user(db: &Connection, name: String) -> rusqlite::Result<i64> {
    db.execute("INSERT INTO user (name) VALUES (?)", [&name])?;

    Ok(db.last_insert_rowid())
}

let mut users = aktor_start(AktorSetup {
    stuck: AktorGroupStuck::KeepRunning,
    actors: AktorNew {
        name: "users".into(),
        kind: AktorKind::TokioThread,
        start: AktorInit::Try(async || open_db()),
        end: close_db,
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::new(128).unwrap(),
        stuck: AktorStuck::Kill,
    },
    shutdown: |report| {
        if report.failed() {
            eprintln!("{report}");
        }
    },
    grace: Duration::from_secs(5),
});

users.ready().await?;

let id = insert_user(users.handles(), "Katten".into()).await??;
```

`users` owns the actor, dropping it runs `close_db`. When startup can't fail you can use `start: AktorInit::Ready(async || state)`.

`aktor_start` itself never waits, the handles are there right away and calls queue until `start` is done. Skip `.ready()` in sync code and a failed start still lands in the report.

## Multiple actors

Actors that belong together go in one start, pass a tuple and the handles come back in the same order:

```rust
let mut started = aktor_start(AktorSetup {
    stuck: AktorGroupStuck::KeepRunning,
    actors: (users_actor, archive_actor),
    shutdown: |report| eprintln!("{report}"),
    grace: Duration::from_secs(5),
});

started.ready().await?;

let (users, archive) = started.handles();
let id = insert_user(users, "Katten".into()).await??;
```

To take a loop handle out of a tuple, use `let (started, (users, capture)) = started.into_parts();`. You can move `capture` into another task and still call `started.stop()`.

Their `start`s run side by side. When `archive`'s start calls `users`, that call just waits until `users` is ready. If you need the users handle to build the archive setup, add it afterwards with `let archive = started.group().spawn(archive_setup).ready().await?;`. The group owns both actors.

Actors stop in reverse startup order, so `archive` closes first and can still call `users` during its `end`.

## Loop actors

A thread that only runs one loop doesn't need calls, a state or an `end`:

```rust
let (started, capture) = aktor_start(AktorSetup {
    stuck: AktorGroupStuck::KeepRunning,
    actors: AktorLoop {
        name: "capture".into(),
        kind: AktorKind::TokioThread,
        work: async move |mut stopping| run_capture(&mut stopping).await,
        stuck: AktorStuck::Leave,
    },
    shutdown: |_| {},
    grace: Duration::from_secs(5),
}).into_parts();

let stopped = started.stop().await;
if stopped.failed() {
    log(stopped);
}

match capture.result().await {
    Ok(frames) => save(frames),
    Err((AktorLoopEnd::Failed(CaptureError::DeviceLost, _), _)) => reconnect(),
    Err((_, report)) => log(report),
}
```

Here `run_capture` returns `Result<Frames, (CaptureError, String)>`. If the loop fails first, its error stops the group and counts even after you read `capture.result()`. If you stopped the group first, reading the loop error marks it delivered. Both `Failed(e, id)` and `Panicked(id)` carry the shutdown row’s ID. If you already handled that failure, compare `row.id != id` when reading `report.failures` to keep the other failures. A cut at grace or a failed start gives you `Cut` or `StartFailed` with the report text.

The loop's tail after `stopping` fires is its cleanup. Under `Leave` it can take as long as it needs, and `result().await` waits for it. Each handle gives you one result. When both the output and the error implement `Clone`, `tokio::spawn(capture.clone().result())` gives another task its own copy.

## Calling another query (transactions)

Inside a `#[aktor]` function you already have the state, so you can pass `&*transaction` or `&db` straight in:

```rust
#[aktor]
pub async fn insert_users(db: &mut Connection, names: Vec<String>) -> rusqlite::Result<()> {
    let transaction = db.transaction()?;

    for name in names {
        insert_user(&*transaction, name).await?;
    }

    transaction.commit()
}
```

The transaction queues once and every insert runs on the connection you already have (never pass your own handle inside one of its functions, that deadlocks).

## Send, cast and timeout

```rust
// Queue now, read reply later (dropping reply leaves it running)
let reply = insert_user(&database, "Katten".into()).send().await?;
let id = reply.await??;

// Send without waiting for a reply (functions returning ())
clear(&counter).cast().await?;

// Timeout on a reply (if it times out, the call keeps running and you can await reply again)
let mut reply = insert_user(&database, "Katten".into()).send().await?;
let id = reply.timeout(Duration::from_secs(2)).await???;
```

## Latest input

For search boxes or sliders where only the newest input matters, you can mark the function `#[aktor(latest)]` and call `.latest()`. New inputs replace pending calls and drop unread results:

```rust
#[aktor(latest)]
async fn find_users(db: &mut Connection, text: String) -> rusqlite::Result<Vec<User>> {
    // ...
}

let (search, mut results) = find_users(&database, "kat".into()).latest();
on_text_changed(move |text| search.send(text));

while let Some(rows) = results.next().await {
    show_rows(rows??);
}
```

Dropping `search` ends the loop after the last result. Losing the actor yields one delivery error and then ends the stream. Only functions marked `latest` generate the session code.

## Roles

If you have two actors holding the same type (like two SQLite `Connection`s) and want Rust to stop you mixing up their queries, you can tag them with a role:

```rust
pub struct MainDb;

#[aktor(role = MainDb)]
pub async fn main_count(db: &Connection) -> rusqlite::Result<u32> {
    // ...
}

let main_db = database.clone().with_role::<MainDb>();
let count = main_count(&main_db).await??;
```

## Background work

For writes that can overlap, give the future to the actor that owns the open files.

```rust
let receipt = storage.spawn(write_body(file, bytes), AktorJob::FinishBeforeEnd)?;
let written = receipt.await?;
```

The actor's capacity also bounds its unfinished jobs. A full set of jobs returns `AktorSpawnError::Full`. Once the mailbox closes, new jobs return `Closing`. Dropping the receipt leaves the write running and its errors still reach the shutdown report.

With `FinishBeforeEnd`, the files stay open until the write finishes. Under `Kill`, a write cut at grace gives you `AktorJobError::Cut(id)` and the report names its spawn site. With `Leave`, shutdown publishes the snapshot and the actor waits for the write before entering `end`. The mailbox is already closed while jobs drain. If the write still needs to call another actor, register that actor first so it stays open.

Inside a call, use `aktor_spawn(work, policy)?`, or `aktor_spawn_local(work, policy)?` for a future holding thread local state on a dedicated thread. Jobs return `()` or `Result<T, X>` where `X` converts into `(E, String)`. Choose `DropAtStop` for work like a periodic refresh that can be dropped as soon as its owner stops.

A blocking device loop can keep a guard for the duration of its wait.

```rust
let _wake = stopping.on_stop(move || waker.wake());
device.wait();
```

The thread closing the actor runs the callback, so it can wake the device even while the actor thread is blocked. Registering after stop runs it immediately.

Use `group.spawn_task(work, AktorJob::FinishBeforeEnd)?` for a task that calls several actors. Register it after the actors it needs so they still answer while it finishes. An unfinished group task gets its grace to finish before shutdown cuts it. A task error or panic stops the group and appears in the report as `ApplicationTask`.

## Shutdown

`started.stop()` closes actors in reverse startup order. Each native thread actor gets `grace` for its calls and, under `Kill`, another `grace` for `end`. A zero grace skips those waits. At app exit, `started.stop().await.into_result()` returns by the group shutdown deadline, even when a `Leave` actor is still closing.

If a resource is dead for good and queued calls would all fail anyway, `users.retire()` starts group shutdown and skips queued calls while letting the current call reply.

Before reopening a device, wait for its actor to let go. That can take as long as the cleanup needs. If a loop returns the open device in its result, close that device too before reopening it.

```rust
let retired = started.stop_released().await?;
let report = retired.report();

if report.failed() {
    eprintln!("{report}");
}

reopen_device()?;
```

Choose `stuck: AktorGroupStuck::KeepRunning` to keep the app running after grace. Native services can choose `AktorGroupStuck::AbortProcess(())` to exit if work gets stuck. A completion lets another task await the grace report with `wait()` or the final report with `wait_retired()`. A synchronous caller can match `retired()` on `Pending`, `Confirmed(retired)` or `Unconfirmed(report)`.

## Setup and cleanup errors

`start`, `end` and loops can return anything that turns into `(E, String)`, your typed error plus the text you want in the report. aktor doesn't care where the pair comes from, so you hand it whatever your functions already return.

With er you don't do anything, an `ErResult` already turns into the pair:

```rust
async fn open_database() -> ErResult<Connection, OpenErr> {
    connect().await.er(())
}

let database = AktorNew {
    name: "db".into(),
    kind: AktorKind::TokioThread,
    start: AktorInit::Try(open_database),
    end: close_database,
    hooks: AktorHooks::NONE,
    capacity: NonZeroUsize::new(32).unwrap(),
    stuck: AktorStuck::Kill,
};
```

With thiserror you make your own error for the actor's code and write the `From` into the pair once. After that your normal `Result` goes in just the same:

```rust
#[derive(Debug, thiserror::Error)]
enum DbError {
    #[error("could not open the database: {0}")]
    Open(#[from] rusqlite::Error),
}

impl From<DbError> for (DbError, String) {
    fn from(error: DbError) -> Self {
        let report = error.to_string();
        (error, report)
    }
}

async fn open_database() -> Result<Connection, DbError> {
    Ok(Connection::open("app.db")?)
}

start: AktorInit::Try(open_database),
```

anyhow can't get that impl, both types are someone else's, so there you build the pair once with `.map_err(|e| { let report = format!("{e:?}"); (e, report) })`. A closure that uses `?` itself has to name its error once, like `async || -> Result<Connection, DbError> { .. }`.

The initial group’s `ready().await` borrows the same receipt or start error for every waiter. Match `AktorStartError::Initializer { error, startup }` for the initializer error, including its tuple position in `AktorBranch`. On either path, look up the handle’s `actor_id()` in the receipt’s `actors` to see how far its initializer got, even when siblings share a name.

For infallible startup, write `start: AktorInit::Ready(async || value)`. A loop with no output or error can return `Ok::<_, (Infallible, String)>(())`. Cleanup can just return `()`.

## Test responses (presets)

In tests you can swap out SQLite with `Preset`:

```rust
use aktor::testing::Preset;

let started = aktor_start(AktorSetup {
    stuck: AktorGroupStuck::KeepRunning,
    actors: Preset::<Connection>::new([insert_user::handler(async |_| Ok(7))]),
    shutdown: |_| {},
    grace: Duration::from_secs(5),
});

assert_eq!(insert_user(started.handles(), "Katten".into()).await??, 7);

started.stop().await.into_result()?;
```

Presets work right next to real actors in a tuple, and adding a handler again overrides the old one.

[App with test presets](https://github.com/Viterkim/aktor/blob/main/aktor/examples/presets.rs)

[SQLite example](https://github.com/Viterkim/aktor/blob/main/aktor/examples/sqlite/main.rs)

[Request example](https://github.com/Viterkim/aktor/blob/main/aktor/examples/guide.rs)
