# Aktor remote

Your service owns the resource. Other processes call its functions over Unix sockets, TCP or UDP. Runs on Unix with Tokio.

```toml
aktor-remote = "0.0.10"
```

## Shared functions

Put the functions in a crate the server and client both use:

```rust
#[aktor(remote)]
pub async fn set_target(power: &mut PowerManager, target: i32) -> i32 {
    power.target = target;
    target
}
```

Derive AktorData on your argument and result types. Plain #[aktor] helpers stay local.

## Server

Start the actor as usual, then let clients use it:

```rust
let actors = aktor_start(power_setup).await?;
let server = Server::listen_with("/run/power.sock", &actors.handles, ServerOptions {
    build: "power-v1".into(),
    ..Default::default()
}).await?;
```

All clients queue on that actor. Close the listener with server.shutdown(), then await actors.shutdown() to finish the work and cleanup.

## Client

```rust
let power = Client::<PowerManager>::with_options("/run/power.sock", ClientOptions {
    build: "power-v1".into(),
    ..Default::default()
})?;

let target = set_target(&power, 10).await?;
```

The ? is for delivery. Use .await?? for a function returning Result.

Calls connect as needed and get one attempt with five seconds. To retry:

```rust
let target = set_target(&power, 10)
    .retry(AktorRetryOpts {
        attempts: 4,
        timeout: Duration::from_secs(5),
    })
    .await?;
```

Four attempts total, five seconds each. A lost reply can mean the work already ran, so retrying can run it twice. The function's own errors don't trigger retries.

## TCP / UDP

Pass Address::Tcp("127.0.0.1:4700".parse()?) on both sides. Address::Udp uses reliable, ordered uTP and needs the udp feature. Neither provides TLS or authentication. Unix sockets default to 0600 permissions.

Change build on both sides when you change transported types, the handshake can't detect reordered fields. The server needs every remote function the client links for that state and role.
