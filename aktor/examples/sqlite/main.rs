use aktor::*;
use core::num::NonZeroUsize;
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
fn open_database() -> std::result::Result<Connection, (OpenErr, String)> {
    Ok(Connection::open_in_memory().er(())?)
}

#[derive(Er)]
pub struct CloseErr;
fn close_database(db: Connection) -> std::result::Result<(), (CloseErr, String)> {
    match db.close() {
        Ok(()) => Ok(()),
        Err((db, err)) => {
            drop(db);
            er_bail!(CloseErr, err)
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ErMain {
    let actors = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "sqlite".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Try(async || open_database()),
            end: close_database,
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(128).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| (),
        grace: Duration::from_secs(5),
    });
    actors
        .ready()
        .await
        .er_val(|(_, report)| ErMainError.er_save("startup", report))?;

    let database = actors.handles();
    schema::create(database).await.er(())?.er(())?;

    let bingo = post::cats::create(database, "Bingo").await.er(())?.er(())?;
    let paws = post::cats::create(database, String::from("Paws"))
        .await
        .er(())?
        .er(())?;

    update::cats::rename(database, paws, "Mittens")
        .await
        .er(())?
        .er(())?;

    let owner = adopt::household(database, "BingoManden", vec![bingo, paws])
        .await
        .er(())?
        .er(())?;

    let cat = get::cats::by_id(database, bingo).await.er(())?.er(())?;
    let household = get::owners::by_id(database, owner).await.er(())?.er(())?;

    println!("{} lives with {}", cat.name, household.name);
    assert_eq!(
        get::cats::by_owner(database, owner).await.er(())?.er(())?,
        [bingo, paws]
    );
    assert_eq!(
        get::owners::by_name(database, "BingoManden")
            .await
            .er(())?
            .er(())?,
        owner
    );

    // The missing cat rolls back the new owner and Bingo's move.
    assert!(
        adopt::household(database, "Haandboldfuglen", vec![bingo, CatId(-1)])
            .await
            .er(())?
            .is_err()
    );
    assert_eq!(get::owners::count(database).await.er(())?.er(())?, 1);
    assert_eq!(
        get::cats::by_id(database, bingo)
            .await
            .er(())?
            .er(())?
            .owner,
        Some(owner)
    );

    actors.stop().await.into_result().er(())?;

    Ok(())
}
