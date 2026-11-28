#[cfg(all(target_family = "wasm", target_os = "unknown"))]
fn worker_setup<C: aktor::AktorWorkerContract, A>(
    name: &str,
    program: &str,
    config: C::Config,
    options: aktor::worker::Options,
    availability: A,
) -> aktor::AktorWorkerNew<C, A> {
    aktor::AktorWorkerNew {
        name: name.into(),
        kind: aktor::AktorKind::BrowserWebWorker::<C>(program),
        config,
        options,
        availability,
        before_shutdown: None,
        stuck: aktor::AktorStuck::Kill,
    }
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
fn group() -> aktor::AktorGroup {
    let mut group = aktor::AktorGroup::with_grace(std::time::Duration::from_secs(1));
    group.start().unwrap();
    group
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
fn options(build: &str) -> aktor::worker::Options {
    aktor::worker::Options {
        build: build.into(),
        capacity: core::num::NonZeroUsize::new(32).unwrap(),
        max_outstanding_bytes: core::num::NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
    }
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod browser;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod group;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod panic;
pub mod preferences;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod remote;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod setup;

#[cfg(test)]
mod tests {
    use super::preferences::{StorageError, read::read, write::write};
    use aktor::{AktorHooks, AktorKind, AktorNew, AktorSetup, AktorStuck, aktor_start};
    use er::*;
    use rusqlite::Connection;
    use std::time::Duration;

    #[tokio::test]
    async fn preferences() {
        #[derive(Er)]
        struct OpenErr;
        #[derive(Er)]
        struct CloseErr;

        let start = async || -> Result<_, (OpenErr, String)> {
            let db = Connection::open_in_memory().er(())?;

            db.execute(
                "CREATE TABLE preferences(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
                [],
            )
            .er(())?;

            Ok(db)
        };
        let db = aktor_start(AktorSetup {
            stuck: aktor::AktorGroupStuck::KeepRunning,
            actors: AktorNew {
                name: "preferences".into(),
                kind: AktorKind::TokioThread,
                start: aktor::AktorInit::Try(start),
                end: async |db: Connection| -> Result<(), (CloseErr, String)> {
                    if let Err((_, error)) = db.close() {
                        er_bail!(CloseErr, error);
                    }

                    Ok(())
                },
                hooks: AktorHooks::NONE,
                capacity: core::num::NonZeroUsize::new(2).unwrap(),
                stuck: AktorStuck::Kill,
            },
            shutdown: |_| {},
            grace: Duration::from_secs(5),
        });
        db.ready().await.unwrap();

        write(db.handles(), "volume".into(), "0.7".into())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            read(db.handles(), "volume".into()).await.unwrap().unwrap(),
            "0.7"
        );
        assert_eq!(
            read(db.handles(), "absent".into()).await.unwrap(),
            Err(StorageError::Missing("absent".into()))
        );
    }
}

#[cfg(all(feature = "wrong-role", target_family = "wasm", target_os = "unknown"))]
mod wrong_role {
    use aktor::*;

    struct Database;
    struct Session;

    impl AktorWorkerContract for Session {
        type State = u32;
        type Role = Session;
        type Config = ();
        type Ready = ();
        type Error = ();
    }

    #[aktor(role = Database)]
    async fn read(state: &u32) -> u32 {
        *state
    }

    async fn mismatched(group: &mut AktorGroup) {
        let worker = group
            .spawn(crate::worker_setup::<Session, _>(
                "session",
                "session.js",
                (),
                crate::options("session"),
                Required,
            ))
            .ready()
            .await
            .unwrap();

        read(&worker).await.unwrap();
    }
}
