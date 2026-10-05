use aktor::*;
use rusqlite::Connection;

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::result::Result<(), Box<dyn core::error::Error>> {
    let setup = async || {
        Connection::open_in_memory().map_err(|error| aktor::AktorSetupError::new(error.to_string()))
    };
    let cleanup = async |db: Connection| {
        db.close()
            .map_err(|(_, error)| aktor::AktorCleanupError::new(error.to_string()))
    };

    let actors = start(AktorSetup {
        name: AktorName::new("sqlite"),
        role: AktorNoRole,
        kind: AktorKind::TokioThread,
        closures: AktorClosures {
            end: Some(cleanup.into()),
            ..AktorClosures::new(setup)
        },
        options: Some(AktorOptions {
            capacity: 128,
            ..Default::default()
        }),
    })
    .await?;

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

    let report = actors.shutdown().await;

    if report.failed() {
        return Err(Box::new(report).into());
    }

    Ok(())
}
