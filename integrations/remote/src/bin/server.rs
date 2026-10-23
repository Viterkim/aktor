use aktor::{AktorArgs, AktorGroup, AktorRunError};
use aktor_remote as remote;
use power_api::{PowerManager, PowerRemote, WIRE_REVISION};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/aktor-power.sock".into());
    let mut actors = AktorGroup::new();
    let _finished = actors.start_with(|_| {})?;
    let power = actors
        .spawn(AktorArgs::new(
            "power",
            PowerManager::open,
            PowerManager::close,
        ))
        .await?
        .with_role::<PowerRemote>();
    let result = async {
        let server = remote::Server::listen_with(
            &path,
            &power,
            remote::ServerOptions {
                build: WIRE_REVISION.into(),
                ..Default::default()
            },
        )
        .await?;
        println!("power service listening on {path}");

        let result = tokio::select! {
            stopped = tokio::signal::ctrl_c() => stopped.map_err(remote::Error::from),
            _ = terminate.recv() => Ok(()),
            result = server.wait() => result,
        };
        let closed = server.shutdown().await;

        result?;
        closed
    }
    .await;
    let report = actors.shutdown().await;

    result?;

    if report.failed() {
        return Err(AktorRunError::from(report).into());
    }

    Ok(())
}
