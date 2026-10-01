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

    let (db, listener) = channel::<Db>(2).unwrap();
    let old = find::request(&db, 0)
        .latest("panel")
        .checked_send()
        .await
        .unwrap();

    let other = fail::request(&db)
        .latest("panel")
        .checked_send()
        .await
        .unwrap();

    let newest = find::request(&db, 0).latest("panel").try_send().unwrap();
    assert_eq!(old.await, Err(CallError::Superseded));

    drop(db);
    listener
        .run(Db {
            rows: vec!["BingoManden".into()],
        })
        .await;

    assert_eq!(other.await, Ok(Err(QueryError)));
    assert_eq!(newest.await.as_deref(), Some("BingoManden"));
}
