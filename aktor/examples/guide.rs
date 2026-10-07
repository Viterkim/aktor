use aktor::listener::{spawn, spawn_local};
use aktor::*;
use rusqlite::Connection;
use std::time::Duration;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Counter(pub u32);

#[aktor]
pub async fn add(counter: &mut Counter, by: u32) -> u32 {
    counter.0 += by;

    counter.0
}

#[aktor]
pub async fn clear(counter: &mut Counter) {
    counter.0 = 0;
}

pub struct Main;

#[aktor(actor = Main)]
pub async fn main_count(counter: &Counter) -> u32 {
    counter.0
}

async fn counters(executor: &tokio::task::LocalSet) -> Result<()> {
    let (handle, task) = spawn_local(executor, Counter(0), 8)?;

    let n = add(&handle, 2).await;
    assert_eq!(n, 2);

    let reply = add(&handle, 3).send().await;

    // do something else

    let n: u32 = reply.await;
    assert_eq!(n, 5);

    clear(&handle).cast().await;

    let reply = add(&handle, 3).send().await;
    let n: u32 = reply.await;
    assert_eq!(n, 3);

    let mut reply = add(&handle, 3).send().await;
    let n = match reply.timeout(Duration::from_millis(20)).await {
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
    let cleanup = |db: Connection| db.close().map_err(|(_, error)| error);

    let (database, actor, thread) = spawn(SpawnArgs {
        name: "sqlite".into(),
        capacity: 128,
        failure: FailurePolicy::Abort,
        setup: Connection::open_in_memory,
        cleanup,
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
