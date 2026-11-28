use aktor::{aktor, testing::Preset};

struct Database {
    connection: rusqlite::Connection,
}

#[derive(Debug, PartialEq)]
struct Row(u32);

#[derive(Debug, PartialEq)]
struct ReadError;

#[aktor(latest)]
async fn load(db: &Database, id: u32) -> Result<Row, ReadError> {
    db.connection
        .query_row("SELECT ?1", [id], |row| row.get(0))
        .map(Row)
        .map_err(|_| ReadError)
}

fn successful() -> Preset<Database> {
    Preset::new([load::handler(async |id| Ok(Row(id)))])
}

#[tokio::test]
async fn independent() {
    let (first, actors) = super::start(successful());
    let second = actors
        .group()
        .spawn(Preset::new([load::handler(async |_| Err(ReadError))]))
        .ready()
        .await
        .unwrap();

    let (updates, mut results) = load(&first, 7).latest();
    drop(updates);
    assert_eq!(results.next().await.unwrap(), Ok(Ok(Row(7))));
    let reply = load(&second, 7).send().await.unwrap();
    drop(second);
    assert_eq!(reply.await, Ok(Err(ReadError)));
    load(&first, 7)
        .timeout(std::time::Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn assertion() {
    for latest in [false, true] {
        let (mock, actors) = super::start(Preset::new([load::handler(async |_| {
            assert_eq!(1, 2, "preset assertion");
            Ok(Row(0))
        })]));
        let caller = tokio::spawn(async move {
            if latest {
                let (_updates, mut results) = load(&mock, 7).latest();
                results.next().await.unwrap().unwrap_err()
            } else {
                load(&mock, 7).await.unwrap_err()
            }
        });
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), caller)
            .await
            .expect("preset assertion left its caller pending")
            .unwrap();
        assert!(error.report.contains("preset assertion"));
        let report = actors.stop().await;
        assert_eq!(error.failure_id, Some(report.failures[0].id));
    }
}

#[tokio::test]
async fn detached() {
    let (mock, actors) = super::start(Preset::new([load::handler(async |_| {
        assert_eq!(1, 2, "detached assertion");
        Ok(Row(0))
    })]));

    drop(load(&mock, 7).send().await.unwrap());

    assert!(actors.stop().await.failed());
}

#[tokio::test]
async fn shared_wire_name() {
    #[aktor(name = "lookup", latest)]
    async fn first(_: &Database) -> u8 {
        1
    }

    #[aktor(name = "lookup", latest)]
    async fn second(_: &Database) -> u8 {
        2
    }

    let (mock, _group) = super::start(Preset::new([
        first::handler(async || 11),
        second::handler(async || 22),
    ]));

    assert_eq!(first(&mock).await, Ok(11));
    let (sender, mut results) = second::latest(&mock);
    sender.send();
    assert_eq!(results.next().await, Some(Ok(22)));
}
