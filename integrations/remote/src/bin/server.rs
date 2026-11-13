use aktor::{AktorKind, AktorNew, AktorSetup, AktorStuck, aktor_start};
use aktor_remote as remote;
use power_api::{PowerManager, PowerRemote, WIRE_REVISION};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/aktor-power.sock".into());
    let started = aktor_start(AktorSetup {
        actors: AktorNew {
            name: "power".into(),
            kind: AktorKind::TokioThread,
            start: async || PowerManager::open(),
            end: async |power: PowerManager| power.close(),
            hooks: Default::default(),
            capacity: 32,
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: std::time::Duration::from_secs(5),
    })
    .await
    .map_err(|(_, report)| report)?;
    let power = started.handles.clone().with_role::<PowerRemote>();
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let server = remote::Server::listen_with(
            &path,
            &power,
            remote::ServerOptions {
                build: WIRE_REVISION.into(),
                ..Default::default()
            },
        )
        .await
        .map_err(|(_, report)| report)?;
        println!("power service listening on {path}");

        let result: Result<(), Box<dyn std::error::Error>> = tokio::select! {
            stopped = tokio::signal::ctrl_c() => stopped.map_err(Into::into),
            _ = terminate.recv() => Ok(()),
            result = server.wait() => result.map_err(|(_, report)| report.into()),
        };
        let closed = server.shutdown().await;

        result?;
        closed.map_err(|(_, report)| report)?;
        Ok(())
    }
    .await;
    let shutdown = started.shutdown().await.into_result();

    result?;
    shutdown.map_err(|(_, report)| report)?;

    Ok(())
}
