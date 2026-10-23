use aktor_remote::{AktorRetryOpts, Client, ClientOptions};
use power_api::{
    PowerManager, PowerRemote, WIRE_REVISION, get_state, power_cycle_gnss, set_target,
};
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("socket path required")?;
    let power = Client::<PowerManager, PowerRemote>::connect_with(
        path,
        ClientOptions {
            build: WIRE_REVISION.into(),
            ..Default::default()
        },
    )
    .await?;
    power_cycle_gnss(&power, 1)
        .await?
        .map_err(|_| "cycle refused")?;
    let (updates, mut results) = set_target(&power, 2)
        .retry(AktorRetryOpts {
            attempts: 50,
            timeout: std::time::Duration::from_secs(1),
        })
        .latest();
    results.next().await.ok_or("latest stream ended")??;
    println!("ready");
    io::stdout().flush()?;

    io::stdin().read_line(&mut String::new())?;
    updates.send(3);
    println!("updating");
    io::stdout().flush()?;
    io::stdin().read_line(&mut String::new())?;
    updates.send(4);
    println!("updated");
    io::stdout().flush()?;
    drop(updates);
    let target = results.next().await.ok_or("latest stream ended")??;
    if target != 4 || results.next().await.is_some() {
        return Err("latest session did not drain its final revision".into());
    }

    power_cycle_gnss(&power, 1)
        .await?
        .map_err(|_| "cycle refused")?;
    let state = get_state(&power).await?;

    if state.cycles != 1 || state.target != 4 {
        return Err("replacement service did not start with fresh state".into());
    }

    println!("reconnected to replacement owner");
    Ok(())
}
