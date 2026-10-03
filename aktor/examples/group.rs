use aktor::*;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
};

type AppError = AktorError;

#[aktor]
async fn append(file: &mut File, text: String) -> io::Result<()> {
    writeln!(file, "{text}")
}

#[tokio::main]
async fn main() {
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

    let run = async move |app: &mut AktorGroup| -> Result<(), AppError> {
        let log = app
            .spawn(ActorArgs {
                name: "log".into(),
                capacity: 32,
                setup,
                cleanup,
            })
            .await
            .map_err(|error| AktorError::new(error.to_string()))?;

        append(&log, "application started".into())
            .await
            .map_err(|error| AktorError::new(error.to_string()))?;

        core::future::pending::<()>().await;
        Ok(())
    };

    let application = AktorGroup::new();
    let kill = application.killswitch();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            kill.stop();
        }
    });

    let outcome = application.run(run, after).await;

    std::process::exit(if outcome.is_err() { 1 } else { 0 });
}
