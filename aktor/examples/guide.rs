use aktor::listener::{spawn, spawn_local};
use aktor::message::TrySendError;
use aktor::*;

struct Counter(u32);

#[aktor]
async fn add(counter: &mut Counter, by: u32) -> u32 {
    counter.0 += by;

    counter.0
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[aktor]
async fn clear(counter: &mut Counter) {
    counter.0 = 0;
}

struct Main;

#[aktor(actor = Main)]
async fn main_count(counter: &Counter) -> u32 {
    counter.0
}

async fn counters(executor: &tokio::task::LocalSet) -> Result<()> {
    let (handle, task) = spawn_local(executor, Counter(0), 8)?;

    let n = add(&handle, 2).await;
    assert_eq!(n, 2);

    let reply = add::request(&handle, 3).send().await;

    // do something else

    let n: u32 = reply.await;
    assert_eq!(n, 5);

    clear::request(&handle).cast().await;

    let admitted = clear::request(&handle).try_cast();
    admitted.map_err(|error| error.to_string())?;

    let reply = match add::request(&handle, 3).try_send() {
        Ok(reply) => reply,
        Err(TrySendError::Full(request)) => request.send().await,
        Err(error) => return Err(error.to_string().into()),
    };

    let n: u32 = reply.await;
    assert_eq!(n, 3);

    use std::time::Duration;
    use tokio::time::timeout;

    let mut reply = add::request(&handle, 3).send().await;
    let n = match timeout(Duration::from_millis(20), &mut reply).await {
        Ok(n) => n,
        Err(_) => reply.await,
    };
    assert_eq!(n, 6);

    let mut local = Counter(0);
    let n: u32 = add(&mut local, 2).await;
    assert_eq!(n, 2);

    drop(handle);
    let counter = task.await?;
    assert_eq!(counter.0, 6);

    Ok(())
}

async fn lifecycle() -> Result<()> {
    use rusqlite::Connection;

    let (database, actor, thread) = spawn(SpawnArgs {
        name: "sqlite".into(),
        capacity: 128,
        failure: FailurePolicy::Abort,
        setup: Connection::open_in_memory,
        cleanup: |db: Connection| db.close().map_err(|(_, error)| error),
    })
    .await?;

    actor.replace(Connection::open_in_memory).await?;

    actor.pause().await?;
    // do something while the database is closed
    actor.resume(Connection::open_in_memory).await?;

    actor.shutdown().await?;
    thread.join_async().await??;
    drop(database);

    Ok(())
}

async fn roles(executor: &tokio::task::LocalSet) -> Result<()> {
    let (handle, task) = spawn_local(executor, Counter(1), 8)?;
    let database = handle.with_role::<Main>();

    let n = main_count(&database).await;
    assert_eq!(n, 1);

    drop(database);
    task.await?;

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            counters(&executor).await?;
            lifecycle().await?;
            roles(&executor).await?;

            Ok(())
        })
        .await
}
