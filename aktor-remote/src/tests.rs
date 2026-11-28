mod framing;
mod latest;
#[cfg(feature = "udp")]
mod udp;

use super::*;
use aktor::{
    Aktor, AktorData, AktorGroup, AktorHooks, AktorKind, AktorNew, AktorSetup, AktorStuck, aktor,
    aktor_start,
};
use core::num::NonZeroUsize;
use er::*;
use std::{
    cell::Cell,
    future::{Future, poll_fn},
    path::Path,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::Poll,
    time::Duration,
};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::Notify,
    task::JoinHandle,
};

fn client_options() -> ClientOptions {
    ClientOptions {
        build: String::new(),
        timeout: Duration::from_secs(5),
        retry_delay: Duration::from_millis(100),
        max_frame: 4 * 1024 * 1024,
    }
}

fn listen_address(address: Address) -> ListenAddress {
    match address {
        Address::Unix(path) => Address::Unix(UnixListenerOptions {
            path,
            permissions: 0o600,
        }),
        Address::Tcp(address) => Address::Tcp(address),
        #[cfg(feature = "udp")]
        Address::Udp(address) => Address::Udp(address),
    }
}

fn server_options() -> ServerOptions {
    ServerOptions {
        build: String::new(),
        max_frame: 4 * 1024 * 1024,
        max_connections: core::num::NonZeroUsize::new(32).unwrap(),
        shutdown_timeout: Duration::from_secs(5),
        io_timeout: Duration::from_secs(5),
    }
}

fn failures(server_error: ServerError) -> Vec<ServerFailure> {
    let ServerError::Shutdown { failures } = server_error else {
        panic!("expected shutdown failures");
    };
    failures
}

struct State {
    value: Rc<Cell<u32>>,
    reads: u32,
    calls: Vec<u32>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct Release(Arc<Notify>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(AktorData, Debug, PartialEq)]
struct Rejected(u32);

#[aktor(remote, latest)]
async fn read(state: &mut State) -> (u32, u32) {
    state.reads += 1;
    (state.value.get(), state.reads)
}

#[aktor(remote, latest)]
async fn increment(state: &mut State, amount: u32) -> Result<u32, Rejected> {
    if amount == 0 {
        return Err(Rejected(17));
    }

    state.value.set(state.value.get() + amount);
    Ok(state.value.get())
}

#[aktor]
async fn gates(state: &State) -> (Arc<Notify>, Arc<Notify>) {
    (state.entered.clone(), state.release.clone())
}

#[aktor(remote, latest)]
async fn crash(_: &mut State) {
    panic!("remote owner failure");
}

#[aktor(remote)]
async fn clear(state: &mut State) {
    state.value.set(0);
}

#[aktor(remote, latest)]
async fn slow(state: &mut State) -> u32 {
    state.value.set(state.value.get() + 1);
    state.entered.notify_one();
    state.release.notified().await;
    999
}

#[aktor(remote)]
async fn slow_unit(state: &mut State) {
    slow(state).await;
}

#[aktor(remote, latest)]
async fn large(_: &State) -> Vec<u8> {
    vec![0; 8192]
}

#[aktor(remote, latest)]
async fn echo(state: &mut State, bytes: Vec<u8>) -> Vec<u8> {
    state.value.set(state.value.get() + 1);
    bytes
}

#[aktor(remote, latest)]
async fn target(state: &mut State, value: u32) -> Result<u32, Rejected> {
    state.calls.push(value);
    if value == 1 {
        state.entered.notify_one();
        state.release.notified().await;
    }

    if value == 0 {
        Err(Rejected(17))
    } else {
        Ok(value)
    }
}

#[aktor]
async fn history(state: &State) -> Vec<u32> {
    state.calls.clone()
}

async fn actor() -> (aktor::AktorStarted<(), AktorGroup>, Aktor<State>) {
    let start = async || {
        Ok::<_, (core::convert::Infallible, String)>(State {
            value: Rc::new(Cell::new(0)),
            reads: 0,
            calls: Vec::new(),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        })
    };
    let power = AktorNew {
        name: "power".into(),
        kind: AktorKind::TokioThread,
        start: aktor::AktorInit::Try(start),
        end: (),
        hooks: AktorHooks::NONE,
        capacity: NonZeroUsize::MIN,
        stuck: AktorStuck::Kill,
    };
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: power,
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    actors.into_parts()
}

#[tokio::test]
async fn network() {
    for udp in [false, true] {
        #[cfg(not(feature = "udp"))]
        if udp {
            continue;
        }

        let (actors, actor) = actor().await;
        let address = "127.0.0.1:0".parse().unwrap();
        let address = if udp {
            #[cfg(feature = "udp")]
            {
                Address::Udp(address)
            }
            #[cfg(not(feature = "udp"))]
            {
                continue;
            }
        } else {
            Address::Tcp(address)
        };
        let server = Server::listen(listen_address(address), &actor, server_options())
            .await
            .unwrap();
        let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();
        assert_eq!(
            increment(&client, 7)
                .send()
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap(),
            7
        );
        let (updates, mut results) = target(&client, 8).latest();
        drop(updates);
        assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 8);

        server.shutdown().await.unwrap();
        let restarted = Server::listen(
            listen_address(server.address.clone()),
            &actor,
            server_options(),
        )
        .await
        .unwrap();
        restarted.shutdown().await.unwrap();
        let _ = actors.stop().await;
    }
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn preset() {
    use aktor::testing::Preset;

    let successful = || Preset::new([increment::handler(async |amount| Ok(amount))]);
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: successful(),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let second = actors
        .group()
        .spawn(Preset::new([increment::handler(async |_| {
            Err(Rejected(88))
        })]))
        .ready()
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let a = directory.path().join("a.sock");
    let b = directory.path().join("b.sock");
    let first = Server::listen(
        listen_address(Address::from(&a)),
        actors.handles(),
        server_options(),
    )
    .await
    .unwrap();
    let second = Server::listen(listen_address(Address::from(&b)), &second, server_options())
        .await
        .unwrap();
    let a = Client::<State>::new(Address::Unix(a), client_options()).unwrap();
    let b = Client::<State>::new(Address::Unix(b), client_options()).unwrap();

    assert_eq!(increment(&a, 7).await.unwrap(), Ok(7));
    assert_eq!(increment(&b, 7).await.unwrap(), Err(Rejected(88)));
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn unavailable() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let error = read(&client).await.unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::NotAdmitted);

    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    assert_eq!(read(&client).await.unwrap().1, 1);
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn admission() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let mut running = Box::pin(slow(&actor).into_future());
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut running => panic!("owner did not block"),
    }
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let sent = tokio::time::timeout(Duration::from_secs(2), increment(&client, 10).send()).await;
    release.notify_one();
    running.await.unwrap();
    let sent = sent.unwrap().unwrap();
    tokio::time::timeout(Duration::from_millis(500), read(&client))
        .await
        .expect("unread reply blocked the next call")
        .unwrap();
    assert_eq!(sent.await.unwrap().unwrap(), 11);

    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn conversion() {
    use aktor::call::CastRequest;

    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let server = Server::listen(
        listen_address(Address::Unix(directory.path().join("power.sock"))),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();

    let unblock = Release(release.clone());
    let running = slow(&actor).send().await.unwrap();
    entered.notified().await;
    let queued = increment(&actor, 10).send().await.unwrap();
    let mut call = increment(&client, 100).into_future();
    assert!(poll_fn(|cx| Poll::Ready(Pin::new(&mut call).poll(cx).is_pending())).await);
    let mut admission = Box::pin(call.send());
    let waiting = tokio::time::timeout(Duration::from_millis(30), &mut admission).await;
    drop(unblock);
    running.await.unwrap();
    queued.await.unwrap().unwrap();
    assert!(waiting.is_err());
    let reply = admission.await.unwrap();
    assert_eq!(reply.await.unwrap(), Ok(111));

    let unblock = Release(release.clone());
    let mut call = slow(&client).into_future();
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut call => panic!("slow call finished early: {result:?}"),
    }
    let reply = call.send().await.unwrap();
    drop(unblock);
    assert_eq!(reply.await.unwrap(), 999);

    let unblock = Release(release.clone());
    let mut call = slow_unit(&client).into_future();
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut call => panic!("slow cast finished early: {result:?}"),
    }
    call.cast().await.unwrap();
    drop(unblock);
    assert_eq!(read(&actor).await.unwrap().0, 113);

    let unblock = Release(release);
    let mut call = slow(&client).into_future();
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut call => panic!("slow call finished early: {result:?}"),
    }
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        call.timeout(Duration::from_millis(10)),
    )
    .await
    .unwrap()
    .unwrap_err();
    drop(unblock);
    assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);

    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn owner_failure() {
    for latest in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        for address in [
            Address::Unix(directory.path().join("power.sock")),
            #[cfg(feature = "udp")]
            Address::Udp("127.0.0.1:0".parse().unwrap()),
        ] {
            let (actors, actor) = actor().await;
            let server = Server::listen(listen_address(address), &actor, server_options())
                .await
                .unwrap();
            let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();
            let answer: std::pin::Pin<Box<dyn std::future::Future<Output = _>>> = if latest {
                let (updates, mut results) = crash(&client).latest();
                Box::pin(async move {
                    let error = results.next().await.unwrap().unwrap_err();
                    assert!(results.next().await.is_none());
                    drop(updates);
                    error
                })
            } else {
                let reply = crash(&client).send().await.unwrap();
                Box::pin(async move { reply.await.unwrap_err() })
            };
            let report = tokio::time::timeout(Duration::from_secs(2), actors.completion().wait())
                .await
                .unwrap()
                .unwrap();
            let failure = &report.failures[0];
            assert!(!failure.delivered());

            let error = tokio::time::timeout(Duration::from_secs(1), answer)
                .await
                .expect("remote call waited past owner failure");
            assert_eq!(error.failure_id, Some(failure.id));
            assert!(error.report.contains("remote owner failure"));
            tokio::time::timeout(Duration::from_secs(2), async {
                while !failure.delivered() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let _ = server.shutdown().await;
        }
    }
}

#[tokio::test]
async fn cast_outcome() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let server = Server::listen(
        listen_address(Address::Unix(directory.path().join("power.sock"))),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();

    crash(&client).cast().await.unwrap();
    let (error, _) = server.shutdown().await.unwrap_err();
    let failed_casts: Vec<_> = failures(error)
        .into_iter()
        .filter(|failure| matches!(failure, ServerFailure::Cast { .. }))
        .collect();
    assert!(matches!(
        failed_casts.as_slice(),
        [ServerFailure::Cast {
            outcome: AktorDeliveryOutcome::OutcomeUnknown,
            cause: Cause::Closed,
            ..
        }]
    ));
    let _ = actors.stop().await;
}

#[tokio::test]
async fn unfinished_cast_at_shutdown() {
    let (actors, actor) = actor().await;
    let (_, release) = gates(&actor).await.unwrap();
    let options = ServerOptions {
        shutdown_timeout: Duration::from_millis(20),
        ..server_options()
    };
    let server = Server::listen(
        Address::Tcp("127.0.0.1:0".parse().unwrap()),
        &actor,
        options,
    )
    .await
    .unwrap();
    let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();

    slow_unit(&client).cast().await.unwrap();
    let (error, _) = server.shutdown().await.unwrap_err();
    release.notify_one();
    assert_eq!(
        failures(error),
        [
            ServerFailure::cast(
                slow_unit::NAME,
                AktorDeliveryOutcome::OutcomeUnknown,
                Cause::Timeout,
                None
            ),
            ServerFailure::ShutdownTimeout,
        ]
    );
    let _ = actors.stop().await;
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn retains_every_failed_cast() {
    let actors = aktor_start(AktorSetup {
        stuck: aktor::AktorGroupStuck::KeepRunning,
        actors: aktor::testing::Preset::new([read::handler(async || (0, 0))]),
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors.ready().await.unwrap();

    let directory = tempfile::tempdir().unwrap();
    let server = Server::listen(
        listen_address(directory.path().join("mock.sock").into()),
        actors.handles(),
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();
    for _ in 0..65 {
        clear(&client).cast().await.unwrap();
    }

    let (error, _) = server.shutdown().await.unwrap_err();
    assert_eq!(
        failures(error),
        vec![
            ServerFailure::cast(
                clear::NAME,
                AktorDeliveryOutcome::Discarded,
                Cause::Protocol,
                None
            );
            65
        ]
    );
    let _ = actors.stop().await;
}

#[tokio::test]
async fn roundtrip() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();

    assert_eq!(increment(&client, 0).await.unwrap(), Err(Rejected(17)));
    increment(&actor, 2).await.unwrap().unwrap();
    increment(&client.clone(), 4).await.unwrap().unwrap();
    drop(client);

    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    assert_eq!(read(&client).await.unwrap().0, 6);
    clear(&client).cast().await.unwrap();
    assert_eq!(read(&client).await.unwrap().0, 0);

    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

async fn proxy_pair(listener: &UnixListener, target: &Path) -> (UnixStream, UnixStream) {
    let (mut client, _) = listener.accept().await.unwrap();
    let mut backend = UnixStream::connect(target).await.unwrap();
    let hello = wire::read(&mut client, wire::MAX_FRAME).await.er_unwrap();
    wire::write(&mut backend, &hello, wire::MAX_FRAME)
        .await
        .er_unwrap();
    let ready = wire::read(&mut backend, wire::MAX_FRAME).await.er_unwrap();
    wire::write(&mut client, &ready, wire::MAX_FRAME)
        .await
        .er_unwrap();
    (client, backend)
}

async fn lose_reply(path: &Path, target: &Path) -> JoinHandle<()> {
    let listener = UnixListener::bind(path).unwrap();
    let target = target.to_owned();

    tokio::spawn(async move {
        let (mut client, mut backend) = proxy_pair(&listener, &target).await;
        let request = wire::read(&mut client, wire::MAX_FRAME).await.er_unwrap();
        wire::write(&mut backend, &request, wire::MAX_FRAME)
            .await
            .er_unwrap();
        if matches!(request, wire::Message::Latest { .. }) {
            let ready = wire::read(&mut backend, wire::MAX_FRAME).await.er_unwrap();
            wire::write(&mut client, &ready, wire::MAX_FRAME)
                .await
                .er_unwrap();
            let update = wire::read(&mut client, wire::MAX_FRAME).await.er_unwrap();
            wire::write(&mut backend, &update, wire::MAX_FRAME)
                .await
                .er_unwrap();
            loop {
                let response = wire::read(&mut backend, wire::MAX_FRAME).await.er_unwrap();
                if matches!(response, wire::Message::LatestAnswer { .. }) {
                    break;
                }
                wire::write(&mut client, &response, wire::MAX_FRAME)
                    .await
                    .er_unwrap();
            }
        } else {
            loop {
                let answer = wire::read(&mut backend, wire::MAX_FRAME).await.er_unwrap();
                if matches!(answer, wire::Message::Answer { .. }) {
                    break;
                }
                wire::write(&mut client, &answer, wire::MAX_FRAME)
                    .await
                    .er_unwrap();
            }
        }
        drop(client);
        connection::finish(backend, Duration::from_secs(2));

        let (mut client, _) = listener.accept().await.unwrap();
        let mut backend = UnixStream::connect(target).await.unwrap();
        let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
    })
}

#[tokio::test]
async fn lost_reply() {
    for retry in [true, false] {
        let (actors, actor) = actor().await;
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("power.sock");
        let proxy = directory.path().join("proxy.sock");
        let server = Server::listen(
            listen_address(Address::from(&socket)),
            &actor,
            server_options(),
        )
        .await
        .unwrap();
        let forwarding = lose_reply(&proxy, &socket).await;
        let client = Client::<State>::new(Address::from(&proxy), client_options()).unwrap();

        if retry {
            assert_eq!(
                increment(&client, 1)
                    .retry(AktorRetryOptions {
                        attempts: core::num::NonZeroUsize::new(2).unwrap(),
                        timeout: Duration::from_secs(2)
                    })
                    .await
                    .unwrap()
                    .unwrap(),
                2
            );
        } else {
            let error = increment(&client, 1).await.unwrap_err();
            assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);
            assert_eq!(read(&actor).await.unwrap().0, 1);
        }

        forwarding.abort();
        let _ = forwarding.await;
        let _ = std::fs::remove_file(proxy);
        server.shutdown().await.unwrap();
        let _ = actors.stop().await;
    }
}

#[tokio::test]
async fn retry_limit() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let proxy = directory.path().join("proxy.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let listener = UnixListener::bind(&proxy).unwrap();
    let (held, received) = tokio::sync::oneshot::channel();
    let forwarding = tokio::spawn(async move {
        let (mut client, mut backend) = proxy_pair(&listener, &socket).await;
        let request = wire::read(&mut client, wire::MAX_FRAME).await.er_unwrap();
        wire::write(&mut backend, &request, wire::MAX_FRAME)
            .await
            .er_unwrap();

        loop {
            let answer = wire::read(&mut backend, wire::MAX_FRAME).await.er_unwrap();
            if matches!(answer, wire::Message::Answer { .. }) {
                held.send(()).unwrap();
                core::future::pending::<()>().await;
                return;
            }

            wire::write(&mut client, &answer, wire::MAX_FRAME)
                .await
                .er_unwrap();
        }
    });
    let delay = Duration::from_secs(5);
    let client = Client::<State>::new(
        Address::from(&proxy),
        ClientOptions {
            retry_delay: delay,
            ..client_options()
        },
    )
    .unwrap();
    let options = AktorRetryOptions {
        attempts: core::num::NonZeroUsize::new(2).unwrap(),
        timeout: Duration::from_secs(1),
    };
    let mut request = increment(&client, 1).retry(options).into_future();
    tokio::select! {
        result = received => result.unwrap(),
        output = &mut request => panic!("withheld reply arrived: {output:?}"),
    }

    tokio::time::pause();
    tokio::time::advance(options.timeout).await;
    // Process the expired timer before polling into backoff.
    tokio::time::sleep(Duration::from_millis(1)).await;
    let backoff = poll_fn(|cx| Poll::Ready(Pin::new(&mut request).poll(cx))).await;
    let mut request = request.retry(AktorRetryOptions {
        attempts: core::num::NonZeroUsize::MIN,
        ..options
    });
    tokio::time::advance(delay).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    let output = poll_fn(|cx| Poll::Ready(Pin::new(&mut request).poll(cx))).await;
    tokio::time::resume();
    forwarding.abort();
    let _ = forwarding.await;
    let value = read(&actor).await.unwrap().0;
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;

    assert!(backoff.is_pending());
    let Poll::Ready(output) = output else {
        panic!("retry limit allowed another attempt")
    };
    let error = output.unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);
    assert_eq!(value, 1);
}

#[tokio::test]
async fn interrupted_write() {
    use tokio::io::AsyncReadExt;

    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let proxy = directory.path().join("proxy.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let listener = UnixListener::bind(&proxy).unwrap();
    let target = socket.clone();

    let forwarding = tokio::spawn(async move {
        let (mut client, _) = listener.accept().await.unwrap();
        let mut backend = UnixStream::connect(&target).await.unwrap();
        let hello = wire::read(&mut client, wire::MAX_FRAME).await.er_unwrap();
        wire::write(&mut backend, &hello, wire::MAX_FRAME)
            .await
            .er_unwrap();
        let ready = wire::read(&mut backend, wire::MAX_FRAME).await.er_unwrap();
        wire::write(&mut client, &ready, wire::MAX_FRAME)
            .await
            .er_unwrap();

        let mut prefix = [0; 8];
        client.read_exact(&mut prefix).await.unwrap();
        use tokio::io::AsyncWriteExt;
        backend.write_all(&prefix).await.unwrap();
        drop(client);
        connection::finish(backend, Duration::from_secs(2));

        let (mut client, _) = listener.accept().await.unwrap();
        let mut backend = UnixStream::connect(target).await.unwrap();
        let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
    });
    let client = Client::<State>::new(Address::from(&proxy), client_options()).unwrap();
    let bytes = vec![23; 1024 * 1024];
    let executions = match echo(&client, bytes.clone())
        .retry(AktorRetryOptions {
            attempts: core::num::NonZeroUsize::new(2).unwrap(),
            timeout: Duration::from_secs(2),
        })
        .await
    {
        Ok(output) => {
            assert_eq!(output, bytes);
            1
        }
        Err(error) => {
            assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);
            0
        }
    };
    assert_eq!(read(&actor).await.unwrap().0, executions);

    forwarding.abort();
    let _ = forwarding.await;
    let (error, _) = server.shutdown().await.unwrap_err();
    assert_eq!(failures(error), [ServerFailure::session(Cause::Connection)]);
    let _ = actors.stop().await;
}

#[tokio::test]
async fn reconnect() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    assert!(
        Server::listen(
            listen_address(Address::from(&socket)),
            &actor,
            server_options()
        )
        .await
        .is_err()
    );
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    server.shutdown().await.unwrap();

    let (reply, server) = tokio::join!(
        increment(&client, 1).retry(AktorRetryOptions {
            attempts: core::num::NonZeroUsize::new(4).unwrap(),
            timeout: Duration::from_secs(1)
        }),
        async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Server::listen(
                listen_address(Address::from(&socket)),
                &actor,
                server_options(),
            )
            .await
            .unwrap()
        }
    );
    assert_eq!(reply.unwrap().unwrap(), 1);

    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn cancellation() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let mut call = Box::pin(slow(&client).into_future());

    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut call => panic!("slow call completed before release"),
    }
    drop(call);
    release.notify_one();

    assert_eq!(read(&client).await.unwrap().0, 1);

    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn deadline() {
    let (actors, actor) = actor().await;
    let (_, release) = gates(&actor).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let error = slow(&client)
        .timeout(Duration::from_millis(50))
        .await
        .unwrap_err();

    assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);
    release.notify_one();

    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn publication() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let long = "nonexistent".repeat(30);
    for (name, limit) in [(history::NAME, wire::MAX_FRAME), (&long, wire::MIN_FRAME)] {
        let mut operations = aktor::worker::Operations::for_remote::<State, ()>();
        operations.0.truncate(1);
        let mut stream = UnixStream::connect(&socket).await.unwrap();
        wire::write(
            &mut stream,
            &wire::Message::Hello {
                version: wire::VERSION,
                build: String::new(),
                operations,
                max_frame: limit as u32,
            },
            wire::MAX_HANDSHAKE,
        )
        .await
        .er_unwrap();
        assert!(matches!(
            wire::read(&mut stream, limit).await.er_unwrap(),
            wire::Message::Ready { .. }
        ));

        wire::write(
            &mut stream,
            &wire::Message::Call {
                id: 1,
                operation: name.into(),
                input: (&[][..]).into(),
            },
            wire::MAX_FRAME,
        )
        .await
        .er_unwrap();
        let wire::Message::Answer {
            output: Err(error), ..
        } = wire::read(&mut stream, limit).await.er_unwrap()
        else {
            panic!("expected a rejection for {name:?}");
        };
        assert_eq!(
            (error.outcome, error.cause),
            (AktorDeliveryOutcome::NotAdmitted, Cause::Protocol)
        );
        assert_eq!(error.reason, Rejection::UnknownOperation);
    }

    let (error, details) = server.shutdown().await.unwrap_err();
    assert_eq!(
        failures(error),
        vec![ServerFailure::session(Cause::Protocol); 2]
    );
    assert!(details.contains(&long[..128]));
    assert!(!details.contains(long.as_str()));
    let _ = actors.stop().await;
}

#[tokio::test]
async fn compatibility() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let incompatible = Client::<State>::new(
        Address::from(&socket),
        ClientOptions {
            build: "different layout".into(),
            ..client_options()
        },
    )
    .unwrap();
    let error = read(&incompatible).await.unwrap_err();
    assert!(error.report.contains("BuildMismatch"));

    let mut stream = UnixStream::connect(&socket).await.unwrap();
    use tokio::io::AsyncWriteExt;
    stream.write_u32(u32::MAX).await.unwrap();
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    assert!(read(&client).await.is_ok());

    let (error, _) = server.shutdown().await.unwrap_err();
    assert_eq!(
        failures(error),
        vec![ServerFailure::session(Cause::Protocol); 2]
    );
    let _ = actors.stop().await;
}

#[tokio::test]
async fn frame_limit() -> ErTest {
    er_assert_err!(
        wire::validate(wire::MIN_FRAME - 1),
        wire::FrameLimitErr::TooSmall { .. }
    );

    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let proxy = directory.path().join("proxy.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        ServerOptions {
            max_frame: 128,
            ..server_options()
        },
    )
    .await
    .unwrap();
    let listener = UnixListener::bind(&proxy).unwrap();
    let forwarding = tokio::spawn(async move {
        let (mut client, mut backend) = proxy_pair(&listener, &socket).await;
        drop(listener);

        tokio::io::copy_bidirectional(&mut client, &mut backend)
            .await
            .unwrap();
    });
    let client = Client::<State>::new(Address::from(&proxy), client_options()).unwrap();

    let error = echo(&client, vec![0; 8192]).await.unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::NotAdmitted);

    let reply = large(&client).send().await.unwrap();
    assert!(reply.await.unwrap_err().report.contains("ReplyTooLarge"));
    assert!(
        echo(&client, vec![0; 8192])
            .await
            .unwrap_err()
            .report
            .contains("Codec")
    );
    assert_eq!(read(&client).await.unwrap().0, 0);

    let (updates, mut results) = large(&client).latest();
    assert!(
        results
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .report
            .contains("Codec")
    );
    drop((updates, results));

    drop(client);
    forwarding.await.unwrap();
    let (error, _) = server.shutdown().await.unwrap_err();
    assert_eq!(
        failures(error),
        vec![ServerFailure::session(Cause::Codec); 2]
    );
    let _ = actors.stop().await;
    Ok(())
}

#[tokio::test]
async fn pending_disconnect() {
    let directory = tempfile::tempdir().unwrap();
    for address in [
        Address::Unix(directory.path().join("power.sock")),
        Address::Tcp("127.0.0.1:0".parse().unwrap()),
        #[cfg(feature = "udp")]
        Address::Udp("127.0.0.1:0".parse().unwrap()),
    ] {
        let (actors, actor) = actor().await;
        let (entered, release) = gates(&actor).await.unwrap();
        let mut running = Box::pin(slow(&actor).into_future());
        tokio::select! {
            _ = entered.notified() => {},
            _ = &mut running => panic!("slow call completed before release"),
        }
        let queued = increment(&actor, 10).send().await.unwrap();

        let server = Server::listen(
            listen_address(address),
            &actor,
            ServerOptions {
                max_connections: core::num::NonZeroUsize::MIN,
                ..server_options()
            },
        )
        .await
        .unwrap();
        let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();
        let mut pending = Box::pin(increment(&client, 100).into_future());
        tokio::select! {
            _ = server.pending.notified() => {},
            _ = &mut pending => panic!("remote request did not wait for admission"),
        }
        drop(pending);
        let cancelled =
            tokio::time::timeout(Duration::from_secs(1), server.cancelled.notified()).await;

        let next = Client::<State>::new(
            server.address.clone(),
            ClientOptions {
                timeout: Duration::from_secs(1),
                ..client_options()
            },
        );

        release.notify_one();
        running.await.unwrap();
        queued.await.unwrap().unwrap();
        cancelled.unwrap();
        read(&next.unwrap()).await.unwrap();
        assert_eq!(read(&actor).await.unwrap().0, 11);

        server.shutdown().await.unwrap();
        let _ = actors.stop().await;
    }
}

#[tokio::test]
async fn listener_stop() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let server = Server::listen(
        listen_address(Address::Unix(directory.path().join("power.sock"))),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();
    let reply = slow(&client).send().await.unwrap();
    entered.notified().await;
    drop(reply);

    tokio::time::timeout(Duration::from_secs(1), server.shutdown())
        .await
        .unwrap()
        .unwrap();
    release.notify_one();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn stopping_admission() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let mut running = Box::pin(slow(&actor).into_future());
    tokio::select! {
        _ = entered.notified() => {},
        _ = &mut running => panic!("owner did not block"),
    }
    let queued = increment(&actor, 10).send().await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let server = Server::listen(
        listen_address(Address::Unix(directory.path().join("power.sock"))),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let client = Client::<State>::new(server.address.clone(), client_options()).unwrap();
    let mut pending = Box::pin(increment(&client, 100).into_future());
    tokio::select! {
        _ = server.pending.notified() => {},
        _ = &mut pending => panic!("remote call did not wait for queue space"),
    }
    let (stopped, output) = tokio::join!(server.shutdown(), &mut pending);
    stopped.unwrap();
    assert_eq!(
        output.unwrap_err().outcome,
        AktorDeliveryOutcome::NotAdmitted
    );
    release.notify_one();
    running.await.unwrap();
    queued.await.unwrap().unwrap();
    assert_eq!(read(&actor).await.unwrap().0, 11);
    let _ = actors.stop().await;
}

async fn raw_client(path: &Path) -> UnixStream {
    let mut stream = UnixStream::connect(path).await.unwrap();
    wire::write(
        &mut stream,
        &wire::Message::Hello {
            version: wire::VERSION,
            build: String::new(),
            operations: aktor::worker::Operations::for_remote::<State, ()>(),
            max_frame: wire::MAX_FRAME as u32,
        },
        wire::MAX_FRAME,
    )
    .await
    .er_unwrap();
    let _ = wire::read(&mut stream, wire::MAX_FRAME).await.er_unwrap();
    stream
}

#[tokio::test]
async fn protocol_deadlines() {
    use tokio::io::AsyncWriteExt;

    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        ServerOptions {
            max_connections: core::num::NonZeroUsize::MIN,
            io_timeout: Duration::from_millis(50),
            ..server_options()
        },
    )
    .await
    .unwrap();
    use tokio::io::AsyncReadExt;
    let mut silent = UnixStream::connect(&socket).await.unwrap();
    assert_eq!(silent.read(&mut [0]).await.unwrap(), 0);

    for prefix in [&[0][..], &[0, 0, 1, 0, 0][..]] {
        let mut partial = raw_client(&socket).await;
        partial.write_all(prefix).await.unwrap();
        assert_eq!(partial.read(&mut [0]).await.unwrap(), 0);
    }

    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    read(&client).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(read(&client).await.is_ok());

    let (mut stalled, _unread) = tokio::io::duplex(32);
    let error = wire::write_reply(&mut stalled, &[0; 1024], Duration::from_millis(50))
        .await
        .err()
        .unwrap();

    assert_eq!(error.top.cause, Cause::Timeout);

    let (error, _) = server.shutdown().await.unwrap_err();
    assert_eq!(
        failures(error),
        vec![ServerFailure::session(Cause::Timeout); 3]
    );
    let _ = actors.stop().await;
}

#[tokio::test]
async fn rejection() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("power.sock");
    let server = Server::listen(
        listen_address(Address::from(&socket)),
        &actor,
        ServerOptions {
            max_frame: 4096,
            ..server_options()
        },
    )
    .await
    .unwrap();
    let mut stream = UnixStream::connect(&socket).await.unwrap();
    let incompatible = "incompatible".repeat(250);
    wire::write(
        &mut stream,
        &wire::Message::Hello {
            version: wire::VERSION,
            build: String::new(),
            operations: aktor::worker::Operations(vec![incompatible.clone()]),
            max_frame: 4096,
        },
        4096,
    )
    .await
    .er_unwrap();

    let response = tokio::time::timeout(Duration::from_secs(2), wire::read(&mut stream, 4096))
        .await
        .unwrap()
        .er_unwrap();
    let wire::Message::Reject(error) = response else {
        panic!("missing rejection")
    };
    assert_eq!(error.cause, Cause::Protocol);
    assert_eq!(error.reason, Rejection::UnsupportedOperation);

    let (_, details) = server.shutdown().await.unwrap_err();
    assert!(details.contains(&incompatible[..128]));
    assert!(!details.contains(incompatible.as_str()));
    let _ = actors.stop().await;
}

#[tokio::test]
async fn owner_shutdown() {
    let (actors, actor) = actor().await;
    let directory = tempfile::tempdir().unwrap();
    let server = Server::listen(
        listen_address(Address::Unix(directory.path().join("power.sock"))),
        &actor,
        server_options(),
    )
    .await
    .unwrap();
    let _ = actors.stop().await;

    assert_eq!(
        failures(server.wait().await.unwrap_err().0),
        [ServerFailure::listener(Cause::Closed)]
    );
}
