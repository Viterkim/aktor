use super::*;
use futures_util::FutureExt;

type DbResult<T> = Result<T, QueryError>;

#[derive(Default)]
pub struct Db {
    pub rows: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct QueryError;

#[aktor]
pub async fn insert(db: &mut Db, row: String) -> usize {
    db.rows.push(row);

    db.rows.len() - 1
}

#[aktor]
pub async fn find(db: &Db, id: usize) -> Option<String> {
    db.rows.get(id).cloned()
}

#[aktor]
pub async fn fail(_db: &Db) -> Result<(), QueryError> {
    Err(QueryError)
}

#[aktor]
pub async fn aliased(db: &Db) -> DbResult<usize> {
    Ok(db.rows.len())
}

#[tokio::test]
async fn typed() {
    let (db, actor) = spawn_thread(Db::default(), 8).unwrap();

    let id: usize = insert(&db, "BingoManden".into()).await;
    let row: Option<String> = find(&db, id).await;

    assert_eq!(row.as_deref(), Some("BingoManden"));
    assert_eq!(fail(&db).await, Err(QueryError));

    let reply: Reply<DbResult<usize>> = aliased::request(&db).send().await;
    assert_eq!(reply.await, Ok(1));

    drop(db);
    actor.join().unwrap();

    let (db, mut listener) = channel::<Db>(2).unwrap();
    struct Panel {
        search: find::LatestSender<aktor::message::LatestSender<(usize,)>>,
    }
    let (search, mut results) = find(&db, 0).latest();
    let panel = Panel { search };
    let (other, mut other_results) = fail(&db).latest();
    panel.search.send(0);
    panel.search.send(1);
    other.send();

    let mut state = Db {
        rows: vec!["BingoManden".into(), "BingoKvinde".into()],
    };
    listener.recv().await.unwrap().run(&mut state).await;
    listener.recv().await.unwrap().run(&mut state).await;
    panel.search.send(0);
    assert!(results.next().now_or_never().is_none());
    listener.recv().await.unwrap().run(&mut state).await;
    assert_eq!(
        results.next().await.flatten().as_deref(),
        Some("BingoManden")
    );
    assert_eq!(other_results.next().await, Some(Err(QueryError)));
    drop(panel);
    assert_eq!(results.next().await, None);
    drop(other);
    assert_eq!(other_results.next().await, None);
}

#[tokio::test]
async fn retained_reply() {
    let (handle, mut listener) = channel::<Db>(2).unwrap();
    let mut state = Db::default();
    let mut reply = insert(&handle, "first".into()).send().await;
    assert!(reply.try_take().is_none());
    listener.recv().await.unwrap().run(&mut state).await;
    assert_eq!(reply.try_take(), Some(0));
    assert!(reply.try_take().is_none());

    let mut reply = insert(&handle, "second".into()).send().await;
    assert!(reply.try_take().is_none());
    listener.recv().await.unwrap().run(&mut state).await;
    assert_eq!(reply.await, 1);
}
