use super::*;

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

#[aktor(latest)]
pub async fn find(db: &Db, id: usize) -> Option<String> {
    db.rows.get(id).cloned()
}

#[aktor(latest)]
pub async fn fail(_db: &Db) -> Result<(), QueryError> {
    Err(QueryError)
}

#[aktor]
pub async fn aliased(db: &Db) -> DbResult<usize> {
    Ok(db.rows.len())
}

#[aktor]
async fn insert_and_find<T: Into<String> + Send + 'static>(db: &mut Db, row: T) -> Option<String> {
    let id = insert(&mut *db, row.into()).await;
    find(&*db, id).await
}

#[tokio::test]
async fn composed_task() {
    let task = tokio::spawn(async {
        let mut db = Db::default();
        insert_and_find(&mut db, "BingoManden").await
    });

    assert_eq!(task.await.unwrap().as_deref(), Some("BingoManden"));
}

#[tokio::test]
async fn typed() {
    let (db, _group) = thread_actor(Db::default());

    let id: usize = insert(&db, "BingoManden".into()).await.unwrap();
    let row: Option<String> = find(&db, id).await.unwrap();

    assert_eq!(row.as_deref(), Some("BingoManden"));
    assert_eq!(fail(&db).await, Ok(Err(QueryError)));

    let reply: Reply<DbResult<usize>> = aliased(&db).send().await.unwrap();

    assert_eq!(reply.await, Ok(Ok(1)));

    drop(db);

    let (db, _group) = thread_actor(Db {
        rows: vec!["BingoManden".into(), "BingoKvinde".into()],
    });

    struct Panel {
        search: find::Sender,
    }

    let (search, mut results) = find(&db, 0).latest();
    let panel = Panel { search };
    let (other, mut other_results) = fail(&db).latest();

    panel.search.send(0);
    panel.search.send(1);
    other.send();
    aliased(&db).await.unwrap().unwrap();

    panel.search.send(0);
    assert_eq!(
        results
            .next()
            .await
            .transpose()
            .unwrap()
            .flatten()
            .as_deref(),
        Some("BingoManden")
    );
    assert_eq!(other_results.next().await, Some(Ok(Err(QueryError))));
    drop(panel);
    assert_eq!(results.next().await, None);
    drop(other);
    assert_eq!(other_results.next().await, None);
}
