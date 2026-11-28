#[path = "../../../cli.rs"]
mod cli;

use aktor::{AktorHooks, AktorKind, AktorNew, AktorSetup, AktorStuck, aktor_start};
use aktor_remote as remote;
use cli::Report;
use power_api::{PowerManager, PowerRemote, WIRE_REVISION};
use std::process::ExitCode;
use tokio::signal::unix::{Signal, SignalKind, signal};

#[tokio::main]
async fn main() -> ExitCode {
    cli::exit(run().await)
}

async fn run() -> Result<(), Report> {
    let mut terminate = signal(SignalKind::terminate())?;
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/aktor-power.sock".into());

    let started = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "power".into(),
            kind: AktorKind::TokioThread,
            start: aktor::AktorInit::Ready(async || PowerManager::open()),
            end: async |power: PowerManager| power.close(),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    });
    started.ready().await?;

    let power = started.handles().clone().with_role::<PowerRemote>();
    let result: Result<(), Report> = async {
        let server = remote::Server::listen(
            remote::Address::Unix(remote::UnixListenerOptions {
                path: path.as_str().into(),
                permissions: 0o600,
            }),
            &power,
            remote::ServerOptions {
                build: WIRE_REVISION.into(),
                max_frame: 4 * 1024 * 1024,
                max_connections: core::num::NonZeroUsize::new(32).unwrap(),
                shutdown_timeout: std::time::Duration::from_secs(5),
                io_timeout: std::time::Duration::from_secs(5),
            },
        )
        .await?;
        println!("power service listening on {path}");

        let result = wait(&server, &mut terminate).await;
        let closed = server.shutdown().await;

        result?;
        closed?;
        Ok(())
    }
    .await;
    let shutdown = started.stop().await.into_result();

    result?;
    shutdown?;

    Ok(())
}

async fn wait(server: &remote::Server, terminate: &mut Signal) -> Result<(), Report> {
    tokio::select! {
        stopped = tokio::signal::ctrl_c() => stopped?,
        _ = terminate.recv() => {},
        result = server.wait() => result?,
    }

    Ok(())
}
