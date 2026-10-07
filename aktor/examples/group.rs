use aktor::*;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
};

#[aktor]
async fn append(file: &mut File, text: String) -> io::Result<()> {
    writeln!(file, "{text}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "aktor.log".into());
    let setup = move || {
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map_err(|error| AktorSetupError::new(error.to_string()))
    };

    let cleanup = |file: File| {
        file.sync_all()
            .map_err(|error| AktorCleanupError::new(error.to_string()))
    };
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
            eprintln!("{error}");
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
        return Err(report.into());
    }

    Ok(())
}
