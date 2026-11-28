use aktor::*;
use core::num::NonZeroUsize;
use er::*;
use rusqlite::Connection;
use std::time::Duration;

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

#[aktor(role = Main)]
pub async fn main_count(counter: &Counter) -> u32 {
    counter.0
}

async fn counters(counter: &Aktor<Counter>) {
    let n = add(counter, 2).await;
    assert_eq!(n, Ok(2));

    let reply = add(counter, 3).send().await.unwrap();

    // do something else

    let n: u32 = reply.await.unwrap();
    assert_eq!(n, 5);

    clear(counter).cast().await.unwrap();

    let reply = add(counter, 3).send().await.unwrap();
    let n: u32 = reply.await.unwrap();
    assert_eq!(n, 3);

    let mut reply = add(counter, 3).send().await.unwrap();
    let n = match reply.timeout(Duration::from_millis(20)).await {
        Ok(n) => n,
        Err(_) => reply.await,
    };
    assert_eq!(n, Ok(6));

    let mut local = Counter(0);
    let n: u32 = add(&mut local, 2).await;
    assert_eq!(n, 2);
}

async fn roles(counter: &Aktor<Counter>) {
    let main = counter.clone().with_role::<Main>();

    let n = main_count(&main).await;
    assert_eq!(n, Ok(6));
}

#[derive(Er)]
pub struct OpenErr;
fn open() -> Result<Connection, (OpenErr, String)> {
    Ok(Connection::open_in_memory().er(())?)
}

#[derive(Er)]
pub struct CloseErr;
fn close(database: Connection) -> Result<(), (CloseErr, String)> {
    if let Err((_, error)) = database.close() {
        er_bail!(CloseErr, error);
    }

    Ok(())
}

#[aktor]
pub async fn sqlite_version(database: &Connection) -> rusqlite::Result<String> {
    database.query_row("SELECT sqlite_version()", [], |row| row.get(0))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ErMain {
    let started = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: (
            AktorNew {
                name: "counter".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Ready(async || Counter(0)),
                end: (),
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "sqlite".into(),
                kind: AktorKind::TokioThread,
                start: AktorInit::Try(async || open()),
                end: close,
                hooks: AktorHooks::NONE,
                capacity: NonZeroUsize::new(32).unwrap(),
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    started
        .ready()
        .await
        .er_val(|(_, report)| ErMainError.er_save("startup", report))?;

    let (counter, database) = started.handles();

    counters(counter).await;
    roles(counter).await;

    let version = sqlite_version(database).await.er(())?.er(())?;
    println!("sqlite {version}");

    started.stop().await.into_result().er(())?;

    Ok(())
}
