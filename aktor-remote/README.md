# Aktor remote

`aktor-remote` is remote IPC with the same patterns as normal `aktor`, letting one service own a resource while other processes call its functions over Unix sockets, TCP or UDP/uTP (Unix + Tokio).

```toml
[dependencies]
aktor = { version = "0.0.13", features = ["tokio"] }
aktor-remote = "0.0.13"
```

## Shared functions

Your `#[aktor(remote)]` functions can live in a shared crate that both the server and client depend on:

```rust
#[aktor(remote)]
pub async fn set_target(power: &mut PowerManager, target: i32) -> i32 {
    power.target = target;
    target
}
```

Your own arg and reply types just need `#[derive(AktorData)]`, and normal `#[aktor]` functions stay local only.

## Server

Start the actor and wait for it to open before accepting clients.

```rust
let power = aktor_start(AktorSetup {
    stuck: aktor::AktorGroupStuck::KeepRunning,
    actors: power_setup,
    shutdown: |report| eprintln!("{report}"),
    grace: Duration::from_secs(5),
});
power.ready().await?;

let socket = Address::Unix(aktor_remote::UnixListenerOptions { path: "/run/power.sock".into(), permissions: 0o600 });
let server = Server::listen(socket, power.handles(), ServerOptions {
    build: "power-v1".into(),
    max_frame: 4 * 1024 * 1024,
    max_connections: core::num::NonZeroUsize::new(32).unwrap(),
    shutdown_timeout: Duration::from_secs(5),
    io_timeout: Duration::from_secs(5),
}).await?;
```

The server example uses `From<(E, String)>` on its application error so `?` carries both startup and listen failures to `main`, where it prints the report once. [Server example](../integrations/remote/src/bin/server.rs)

Every connected client queues work on the same actor. Pass a testing `Mock` to the same listener to run those calls against its handlers.

Call `server.shutdown().await?` before stopping the actor group to let admitted work finish. On failure, `ServerError::Shutdown { failures }` contains each failed cast, session and listener error since startup. The accompanying report keeps the details.

## Client

```rust
let socket = Address::Unix("/run/power.sock".into());
let power = Client::<PowerManager>::new(socket, ClientOptions {
    build: "power-v1".into(),
    timeout: Duration::from_secs(5),
    retry_delay: Duration::from_millis(100),
    max_frame: 4 * 1024 * 1024,
})?;

let target = set_target(&power, 10).await?;
```

A delivery failure gives you an `AktorDeliveryError`. Its `outcome` tells you whether the call was refused, discarded, or might have run. If the owner failed, the report and `failure_id` identify that failure on the server. A successful delivery gives you whatever your function returned, including its own `Result`. A latest stream ends after a delivery error. Open a new session on the same client to send updates to a replacement owner.

The first call opens the connection. Each call gets one attempt with the timeout in `ClientOptions`. Give it more attempts when the operation is safe to repeat.

```rust
let target = set_target(&power, 10)
    .retry(AktorRetryOptions {
        attempts: core::num::NonZeroUsize::new(4).unwrap(),
        timeout: Duration::from_secs(5),
    })
    .await?;
```

`attempts` includes the first try, and a lost reply can mean the call already ran so retries might run it again (your function's own `Err` doesn't trigger a retry).

## TCP / UDP

For TCP you can pass `Address::Tcp("127.0.0.1:4700".parse()?)` on both sides, or `Address::Udp` for reliable ordered uTP (needs the `udp` feature). Neither TCP nor uTP has TLS or auth, and `ServerOptions.permissions` sets the Unix socket mode.

Whenever you change wire types you should bump `build` on both sides (the handshake checks function names and types, but reordering fields can still change the bytes).
