use aktor::*;
use er::*;
use std::{
    fs::{File, OpenOptions},
    io::Write,
};

#[derive(Er)]
pub struct AppendErr;
#[aktor]
async fn append(file: &mut File, text: String) -> ErResult<(), AppendErr> {
    writeln!(file, "{text}").er(())
}

#[derive(Er)]
pub struct OpenErr(pub String);
fn open_log(path: &str) -> ErResult<File, OpenErr> {
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .er(|_| path)
}

#[derive(Er)]
pub struct SyncErr;
fn sync_log(file: File) -> ErResult<(), SyncErr> {
    file.sync_all().er(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "aktor.log".into());
    let setup = move || open_log(&path).er_into(|report| report);
    let cleanup = |file| sync_log(file).er_into(|report| report);
    let after = async |report: ShutdownReport| {
        if report.failed() {
            eprintln!("{report}");
        }

        println!("application cleanup finished");
        Ok::<_, AktorCleanupError>(())
    };

    let mut actors = AktorGroup::new();
    let kill = actors.killswitch();
    let closing = actors.start_with(after)?;
    let log = actors.spawn(ActorArgs::new("log", setup, cleanup)).await?;

    actors.spawn_task(async move {
        if let Err(error) = append(&log, "application started".into()).await {
            eprintln!("{}", error.er_report());
        }

        core::future::pending::<()>().await;
    })?;

    let ctrl_c = kill.clone();
    actors.spawn_task(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            ctrl_c.stop();
        }
    })?;

    kill.wait_stopping().await;
    let report = closing.await;

    if report.failed() {
        return Err(aktor::AktorRunError::from(report).into());
    }

    Ok(())
}
