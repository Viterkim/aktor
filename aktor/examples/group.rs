use aktor::*;
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
fn open_log(path: &str) -> ErResult<File, OpenErr> {
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .er(path)
}

#[derive(Er)]
pub struct SyncErr;
fn sync_log(file: File) -> ErResult<(), SyncErr> {
    file.sync_all().er(())
}

#[derive(Er)]
pub struct LogUntilStopErr;
async fn log_until_stop(log: &Aktor<File>) -> ErResult<(), LogUntilStopErr> {
    append(log, "application started".into()).await.er(())?;

    tokio::signal::ctrl_c().await.er(())
}

#[tokio::main]
async fn main() -> ErMain {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "aktor.log".into());

    let started = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "log".into(),
            kind: AktorKind::TokioThread,
            start: async move || open_log(&path),
            end: sync_log,
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: async |report| {
            if report.failed() {
                eprintln!("{report}");
            }

            println!("application cleanup finished");
        },
        grace: Duration::from_secs(5),
    })
    .await
    .er_report()?;

    let logged = started
        .killswitch()
        .run(log_until_stop(&started.handles))
        .await;

    if let Some(Err(error)) = logged {
        eprintln!("{}", error.er_report());
    }

    started.shutdown().await;

    Ok(())
}
