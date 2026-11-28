# Browser SQLite (Web Workers)

This lets you run an actor (like SQLite) inside its own browser Web Worker so it doesn't block the UI thread, using the same `#[aktor]` functions as native code.

Put the contract next to the database state so the page and worker share it. Here the page sends an `OpenConfig` and gets back an `Opened` receipt or an `OpenErr`. Derive `AktorData` on all three so they can cross the worker boundary.

```rust
struct MainDb;

impl AktorWorkerContract for MainDb {
    type State = Database;
    type Role = MainDb;
    type Config = OpenConfig;
    type Ready = Opened;
    type Error = OpenErr;
}
```

With `wasm_browser_workers` on the page, the contract tells `group.spawn` which config to accept.

```rust
let db = group.spawn(AktorWorkerNew {
    name: "database".into(),
    kind: AktorKind::BrowserWebWorker::<MainDb>("/database-worker.js"),
    config,
    availability: aktor::Required,
    before_shutdown: None,
    options: worker::Options {
        build: "database-v1".into(),
        capacity: NonZeroUsize::new(32).unwrap(),
        max_outstanding_bytes: core::num::NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
    },
    stuck: AktorStuck::Kill,
});
```

Enable `wasm_browser_worker_server` in the worker crate so the call bodies only get linked there. Open SQLite in `start` and return the receipt along with the connection the worker will serve.

```rust
worker::serve_setup::<MainDb>(options, |config| {
    AktorServe {
        start: AktorInit::Try(async move || -> ErResult<_, OpenErr> {
            let (db, opened) = opening(config).await?;

            Ok(AktorReady { state: db, ready: opened })
        }),
        end,
        hooks,
    }
})
.await?;
```

A worker with nothing to send uses `type Ready = ();` and returns the state from `start`. The bootstrap awaits `serve_setup` until cleanup finishes and receives its final result.

On the page, `db.ready().await` borrows the worker’s ready payload. If opening the database failed, match `WorkerStartError::Initializer { error, .. }` for its `OpenErr`. Use `db` for ordinary actor calls.

```rust
#[derive(aktor::AktorData)]
struct Record {
    key: String,
    bytes: Vec<u8>,
}

#[derive(Er, aktor::AktorData)]
struct SaveErr {
    key: String,
}

#[aktor::aktor(role = MainDb)]
async fn save(db: &mut Database, record: Record) -> Result<(), (SaveErr, String)> {
    db.save(record).await
}

let worker = db.ready().await?;
let opened = worker.ready().await?;
save(&worker, record).await??;
```

The role picks which functions this worker serves when two workers share a state type. Use `type Role = ();` for plain `#[aktor]` functions. Derive `AktorData` on the owned arguments and result. Here `db.save` returns an `ErResult<(), SaveErr>` and the macro converts its error into the pair in the signature. The page only compiles the worker call, so `Database::save` can live entirely in the worker.

Whenever you change types that cross the worker boundary you should bump the build string on both sides (`options.build` on `AktorWorkerNew` and `worker::Options.build` in the worker).

## Optional workers

Set `availability: aktor::Optional` for an archive whose failure should let preferences keep running. The setup now gives you an `OptionalWorker<Archive>`.

```rust
let archive = group.spawn(archive_setup).ready().await?;
save(&archive, record).await??;
```

You still get a handle even if the worker failed to start (you can check `archive.ready().await` if you care why), and the group stays up.

## Shutdown

Since calls and `end` run inside the worker, the page can't cancel them while they're running, so the worker gets both grace periods together. If time runs out, `AktorStuck::Kill` terminates the worker while `AktorStuck::Leave` lets it finish accepted calls and cleanup.

When an archive on the page still uses the database, set `before_shutdown: Some(worker::join(join_archive()))`. Return `WorkerJoinOutcome::Stopped(result)` once the archive has stopped, even if it failed. Return `Unconfirmed(report)` when it could still use storage. That keeps the database open through forced shutdown too.

Inside the worker, return `Err(worker::ServeFailure::Pending { error, report, finish })` from `start` or `end` to tell the page that release is pending. Move the resources into the boxed `finish` future. It can keep trying, or stay pending while it holds them.

Match `completion.cleanup()` on `Pending`, `Returned(result)` or `Unconfirmed(receipt)`. You can check the status as often as you like. Call `receipt.take()` when handling an error so the group knows you received it.

## Build

You'll need the `wasm32-unknown-unknown` target and `wasm-bindgen-cli` matching the `wasm-bindgen` version in `integrations/Cargo.lock`, then from the repo root:

```sh
bash integrations/worker/build.sh
```

Then you can serve `integrations/worker/web`.

When several workers share a crate, the server feature normally links the registered call bodies for every state. Give each worker its own Cargo invocation and `--target-dir`. Build the counter worker with `RUSTFLAGS='--cfg aktor_serve="Counter"'` and `--target-dir target/counter`. Put the flags on the whole Cargo command so dependencies that declare calls see them too.

The selector matches the last name in the state type as written on each `#[aktor]` function, so `archive::codec::State` matches `State`. Other states keep their signatures for the page's handshake. Leave the selector off the page build.

[Example](src/browser.rs)
