use aktor::*;
use er::*;
use rusqlite::Connection;
use std::time::Duration;

pub mod adopt;
pub mod get;
pub mod post;
pub mod schema;
pub mod update;

pub type Result<T> = rusqlite::Result<T>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatId(pub i64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerId(pub i64);

#[derive(Debug, PartialEq, Eq)]
pub struct Cat {
    pub id: CatId,
    pub name: String,
    pub owner: Option<OwnerId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Owner {
    pub id: OwnerId,
    pub name: String,
}

#[derive(Er)]
pub struct OpenErr;
fn open_database() -> ErResult<Connection, OpenErr> {
    Connection::open_in_memory().er(())
}

#[derive(Er)]
pub struct CloseErr;
fn close_database(db: Connection) -> ErResult<(), CloseErr> {
    match db.close() {
        Ok(()) => Ok(()),
        Err((db, err)) => {
            drop(db);
            Err(err.er(()))
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ErMain {
    let actors = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "sqlite".into(),
            kind: AktorKind::TokioThread,
            start: async || open_database(),
            end: close_database,
            hooks: Default::default(),
            capacity: 128,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| (),
        grace: Duration::from_secs(5),
    })
    .await
    .er_test()?;

    let database = &actors.handles;
    schema::create(database).await?;

    let bingo = post::cats::create(database, "Bingo").await?;
    let paws = post::cats::create(database, String::from("Paws")).await?;

    update::cats::rename(database, paws, "Mittens").await?;

    let owner = adopt::household(database, "BingoManden", vec![bingo, paws]).await?;

    let cat = get::cats::by_id(database, bingo).await?;
    let household = get::owners::by_id(database, owner).await?;

    println!("{} lives with {}", cat.name, household.name);
    assert_eq!(get::cats::by_owner(database, owner).await?, [bingo, paws]);
    assert_eq!(get::owners::by_name(database, "BingoManden").await?, owner);

    // The missing cat rolls back the new owner and Bingo's move.
    assert!(
        adopt::household(database, "Haandboldfuglen", vec![bingo, CatId(-1)])
            .await
            .is_err()
    );
    assert_eq!(get::owners::count(database).await?, 1);
    assert_eq!(get::cats::by_id(database, bingo).await?.owner, Some(owner));

    actors.shutdown().await.into_result().er_test()?;

    Ok(())
}
