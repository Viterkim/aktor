# Examples

The native examples use the `tokio` feature. Put several actors in a tuple to get their handles back in the same order. The [README's SQLite example](https://github.com/Viterkim/aktor#sqlite) has the `insert_user` used below.

With `use er::*;`, `.er(())?` keeps both the startup error and the report. When you match `failure.init` on a tuple startup error, `AktorInitOutcome::Failed(AktorBranch::Second(error))` gives you the second actor's initializer error.

```rust
let started = aktor_start(AktorSetup {
    actors: (users_actor, archive_actor),
    shutdown: |report| eprintln!("{report}"),
    grace: Duration::from_secs(5),
})
.await
.er(())?;

let (users, archive) = &started.handles;
let id = insert_user(users, "Katten".into()).await?;
```

When an actor needs another actor's handle to start, add it with `started.group.spawn(..)` once that handle is ready. Actors stop in reverse order, so `archive` goes first and can still write to `users` in its `end`.

## Shutdown

`started.shutdown().await` closes the group and gives you its report. Aktor gives each actor `grace` to finish its calls, then cancels calls that can yield. Its `end` gets another `grace`. Browser workers get both periods at once. A browser thread with `AktorStuck::Leave` keeps draining its calls after the first grace so accepted work can finish.

Call `users.retire()` when the database connection is gone for good and queued calls would fail too. That starts group shutdown and tells this dedicated thread to skip its queued calls. A call that retires itself can still return its final reply.

If cleanup needs the outcome of a call, use `.settle().await` on a dedicated thread or a required browser worker. An optional worker's ordinary `.await` already gives you a delivery result. Use `.admit().await` when another task needs to keep the reply and call `receive_settled().await` later.

Do your own cleanup before calling another actor from `end`. A call into the actor that died might never come back.

A native thread stuck in blocking code can't be killed on its own. By default the group gives you its shutdown report and leaves the app running. This works even when the group's own `shutdown` callback blocks. A service that needs a hard exit can choose `aktor_start_with_policy(setup, AktorShutdownPolicy::AbortProcess)` at startup. Then a stuck actor marked `Kill` or a stuck group callback can take down the process.

```rust
let (users, _) = &started.handles;
let database = users.completion();
let closing = started.shutdown();

closing.wait_retired().await.er(())?;
database.wait_cleanup().await.er(())?;
replace_database_file().er(())?;
```

The database owner can run on a dedicated thread or in a browser worker. You need both answers before replacing its file. The owner can stop even if SQLite refused to close the database. The report from `wait_retired()` can include cleanup failures that arrived after the first shutdown report.

While the UI is closing, `closing.retirement_status()` lets you read the current status without copying the shutdown report. Even with `AllConfirmed`, check `database.cleanup_status()` before replacing the file.

## Calling another query

```rust
#[derive(Er)]
pub struct InsertUsersErr;

#[aktor]
pub async fn insert_users(db: &mut Connection, names: Vec<String>) -> ErResult<(), InsertUsersErr> {
    let transaction = db.transaction().er(())?;

    for name in names {
        insert_user(&*transaction, name).await.er(())?;
    }

    transaction.commit().er(())
}
```

The transaction queues once. Every insert runs right there on the connection it already has.

## Get the reply later

```rust
let reply = insert_user(&database, "Katten".into()).send().await;

// do other work
let id = reply.await?;
```

`.send().await` waits for queue space. Dropping the reply leaves the operation running. Use `.cast().await` to send a function that returns `()` and skip the reply.

## Latest input

Give the search box the sender so an edit can replace a pending search. The new input also discards any unread result from the last search.

```rust
let (search, mut results) = find_users(&database, "kat".into()).latest();
on_text_changed(move |text| search.send(text));

while let Some(rows) = results.next().await {
    show_rows(rows?);
}
```

When the search box drops its sender, the loop can finish after the last result. Use ordinary calls for writes where each input counts.

## Test responses

Skip opening SQLite and give the function a test response. In a test returning `ErTest`, use `.er_test()?` on startup and shutdown results so a failure prints the report.

```rust
use aktor::testing::Preset;
use er::*;

let started = aktor_start(AktorSetup {
    actors: Preset::<Connection>::new().on(insert_user::handler(async |_| Ok(7))),
    shutdown: |_| {},
    grace: Duration::from_secs(5),
})
.await
.er_test()?;

assert_eq!(insert_user(&started.handles, "Katten".into()).await?, 7);

started.shutdown().await.into_result().er_test()?;
```

You can put a preset next to real actors in the tuple and replace one response by adding its handler again. A missing handler can fail the group even if you dropped its reply. Checking the report in the test catches that too.

[App with test presets](https://github.com/Viterkim/aktor/blob/main/aktor/examples/presets.rs)

## Setup and cleanup errors

Return the database's `ErResult` straight from `start`. Match `failure.init` on `AktorInitOutcome::Failed(error)` to get the original `OpenErr` and keep the report text for its causes. With `NotRun`, none of the requested initializers ran. With `Unconfirmed`, one may already have opened the file. Calling `.er_report()` inside `start` would throw that type away.

```rust
async fn open_database() -> ErResult<Connection, OpenErr> {
    connect().await.er(())
}

let database = AktorNew {
    start: open_database,
    end: close_database,
    // ...
};
```

If both databases fail to close, both errors go into the report. On a dedicated thread or a browser worker, `wait_cleanup()` gives you the result of `end`. If a window needs to keep responding while the database closes, `cleanup_status()` tells it whether cleanup is pending, succeeded, failed or went unconfirmed. A library can return `Result<(), (CloseErr, String)>` from `close_database`, even if it uses a different version of Er or no Er at all.

An initializer that can't fail can return `aktor_ready(state)`. Use `end: ()` to drop the state. If you have cleanup work that can't fail, the function can just return `()`.

[SQLite example](https://github.com/Viterkim/aktor/blob/main/aktor/examples/sqlite/main.rs)

[Request example](https://github.com/Viterkim/aktor/blob/main/aktor/examples/guide.rs)
