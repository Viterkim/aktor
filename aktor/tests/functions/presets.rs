use aktor::{AktorGroup, aktor, testing::Preset};

struct Database {
    connection: rusqlite::Connection,
}

#[derive(Debug, PartialEq)]
struct Row(u32);

#[derive(Debug, PartialEq)]
struct ReadError;

#[aktor]
async fn load(db: &Database, id: u32) -> Result<Row, ReadError> {
    db.connection
        .query_row("SELECT ?1", [id], |row| row.get(0))
        .map(Row)
        .map_err(|_| ReadError)
}

fn successful() -> Preset<Database> {
    Preset::new().on(load::handler(async |id| Ok(Row(id))))
}

#[tokio::test]
async fn independent() {
    let mut actors = AktorGroup::new();
    let _completion = actors.start_with(|_| {}).unwrap();
    let first = successful().start(&mut actors).await.unwrap();
    let second = successful()
        .on(load::handler(async |_| Err(ReadError)))
        .start(&mut actors)
        .await
        .unwrap();

    let (updates, mut results) = load(&first, 7).latest();
    drop(updates);
    assert_eq!(results.next().await.unwrap(), Ok(Row(7)));
    let reply = load(&second, 7).send().await;
    drop(second);
    assert_eq!(reply.await, Err(ReadError));
    load(&first, 7)
        .timeout(std::time::Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    actors.shutdown().await;
}

#[tokio::test]
async fn assertion() {
    for latest in [false, true] {
        let mut actors = AktorGroup::new();
        let _completion = actors.start_with(|_| {}).unwrap();
        let mock = successful()
            .on(load::handler(async |_| {
                assert_eq!(1, 2, "preset assertion");
                Ok(Row(0))
            }))
            .start(&mut actors)
            .await
            .unwrap();
        let caller = tokio::spawn(async move {
            if latest {
                let (_updates, mut results) = load(&mock, 7).latest();
                let _ = results.next().await;
            } else {
                let _ = load(&mock, 7).await;
            }
        });
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), caller)
            .await
            .expect("preset assertion left its caller pending")
            .unwrap_err();
        assert!(error.is_panic());
        actors.shutdown().await;
    }
}

#[tokio::test]
async fn detached() {
    let mut actors = AktorGroup::new();
    let _completion = actors.start_with(|_| {}).unwrap();
    let mock = successful()
        .on(load::handler(async |_| {
            assert_eq!(1, 2, "detached assertion");
            Ok(Row(0))
        }))
        .start(&mut actors)
        .await
        .unwrap();

    drop(load(&mock, 7).send().await);

    assert!(actors.shutdown().await.failed());
}
