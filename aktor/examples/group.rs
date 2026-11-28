use aktor::*;
use core::num::NonZeroUsize;
use er::*;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    time::Duration,
};

#[derive(Er)]
pub struct AppendErr;
#[aktor]
async fn append(file: &mut File, text: String) -> ErResult<(), AppendErr> {
    writeln!(file, "{text}").er(())
}

#[derive(Er)]
pub struct OpenErr {
    pub path: PathBuf,
}
fn open_log(path: &str) -> Result<File, (OpenErr, String)> {
    Ok(OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .er(path)?)
}

#[derive(Er)]
pub struct SyncErr;
fn sync_log(file: File) -> Result<(), (SyncErr, String)> {
    file.sync_all().er(())?;
    Ok(())
}

#[derive(Er)]
pub struct LogUntilStopErr;
async fn log_until_stop(log: &Aktor<File>) -> ErResult<(), LogUntilStopErr> {
    append(log, "application started".into())
        .await
        .er(())?
        .er(())?;

    tokio::signal::ctrl_c().await.er(())
}

#[tokio::main]
async fn main() -> ErMain {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "aktor.log".into());

    let started = aktor_start(AktorSetup {
        stuck: AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "log".into(),
            kind: AktorKind::TokioThread,
            start: AktorInit::Try(async move || open_log(&path)),
            end: sync_log,
            hooks: AktorHooks::NONE,
            capacity: NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: async |_| println!("application cleanup finished"),
        grace: Duration::from_secs(5),
    });
    started
        .ready()
        .await
        .er_val(|(_, report)| ErMainError.er_save("startup", report))?;

    let logged = started
        .killswitch()
        .run(log_until_stop(started.handles()))
        .await;

    if let Some(Err(error)) = logged {
        eprintln!("{}", error.er_report());
    }

    started.stop().await.into_result().er(())?;

    Ok(())
}
