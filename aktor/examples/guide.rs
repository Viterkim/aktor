use aktor::*;
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
    assert_eq!(n, 2);

    let reply = add(counter, 3).send().await;

    // do something else

    let n: u32 = reply.await;
    assert_eq!(n, 5);

    clear(counter).cast().await;

    let reply = add(counter, 3).send().await;
    let n: u32 = reply.await;
    assert_eq!(n, 3);

    let mut reply = add(counter, 3).send().await;
    let n = match reply.timeout(Duration::from_millis(20)).await {
        Ok(n) => n,
        Err(_) => reply.await,
    };
    assert_eq!(n, 6);

    let mut local = Counter(0);
    let n: u32 = add(&mut local, 2).await;
    assert_eq!(n, 2);
}

async fn roles(counter: &Aktor<Counter>) {
    let main = counter.clone().with_role::<Main>();

    let n = main_count(&main).await;
    assert_eq!(n, 6);
}

#[derive(Er)]
pub struct OpenErr;
fn open() -> ErResult<Connection, OpenErr> {
    Connection::open_in_memory().er(())
}

#[derive(Er)]
pub struct CloseErr;
fn close(database: Connection) -> ErResult<(), CloseErr> {
    database.close().map_err(|(_, error)| error).er(())
}

#[aktor]
pub async fn sqlite_version(database: &Connection) -> rusqlite::Result<String> {
    database.query_row("SELECT sqlite_version()", [], |row| row.get(0))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ErMain {
    let started = aktor_start(AktorSetup {
        actors: (
            AktorNew {
                name: "counter".into(),
                kind: AktorKind::TokioThread,
                start: async || aktor_ready(Counter(0)),
                end: (),
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
            AktorNew {
                name: "sqlite".into(),
                kind: AktorKind::TokioThread,
                start: async || open(),
                end: close,
                hooks: Default::default(),
                capacity: 32,
                stuck: AktorStuck::Kill,
            },
        ),
        shutdown: |report| {
            if report.failed() {
                eprintln!("{report}");
            }
        },
        grace: Duration::from_secs(5),
    })
    .await
    .er_test()?;

    let (counter, database) = &started.handles;

    counters(counter).await;
    roles(counter).await;

    assert!(sqlite_version(database).await.is_ok());

    started.shutdown().await;

    Ok(())
}
