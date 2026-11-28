use super::*;
use aktor::data::{DataError, Reader, Writer};
use tokio::time::{sleep, timeout};

struct Counted {
    value: u32,
    count: Arc<std::sync::atomic::AtomicUsize>,
}

impl AktorData for Counted {
    fn encode_data(&self, writer: &mut Writer) -> Result<(), DataError> {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        writer.write(&self.value)
    }

    fn decode_data(reader: &mut Reader<'_>) -> Result<Self, DataError> {
        Ok(Self {
            value: reader.read()?,
            count: Arc::default(),
        })
    }
}

#[aktor(remote, latest)]
async fn counted(_: &State, value: Counted) -> u32 {
    value.value
}

#[tokio::test]
async fn encoding() {
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
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (updates, mut results) = counted(
        &client,
        Counted {
            value: 1,
            count: count.clone(),
        },
    )
    .latest();
    for value in 2..=1000 {
        updates.send(Counted {
            value,
            count: count.clone(),
        });
    }
    results.next().await.unwrap().unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);

    drop((updates, results));
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn queued() {
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
    let (updates, mut results) = target(&client, 2).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();
    let cloned = updates.clone();
    cloned.send(3);
    updates.send(4);
    timeout(Duration::from_secs(2), results.seen(3))
        .await
        .unwrap();
    drop((updates, cloned));

    release.notify_one();
    running.await.unwrap();
    assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 4);
    assert!(results.next().await.is_none());
    assert_eq!(history(&actor).await.unwrap(), [4]);

    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn running() {
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
    let (updates, mut results) = target(&client, 1).latest();
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    for value in 2..=1000 {
        updates.send(value);
    }
    timeout(Duration::from_secs(2), results.seen(1000))
        .await
        .unwrap();
    release.notify_one();

    assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 1000);
    assert_eq!(history(&actor).await.unwrap(), [1, 1000]);
    updates.send(0);
    assert_eq!(results.next().await.unwrap().unwrap(), Err(Rejected(17)));

    drop((updates, results));
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn independent() {
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
    let (first, mut a) = target(&client, 2).latest();
    let (second, mut b) = target(&client, 3).latest();
    timeout(Duration::from_secs(2), a.seen(1)).await.unwrap();
    timeout(Duration::from_secs(2), b.seen(1)).await.unwrap();
    first.send(4);
    timeout(Duration::from_secs(2), a.seen(2)).await.unwrap();

    release.notify_one();
    running.await.unwrap();
    assert_eq!(
        (
            a.next().await.unwrap().unwrap().unwrap(),
            b.next().await.unwrap().unwrap().unwrap()
        ),
        (4, 3)
    );

    drop((first, second, a, b));
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn receiver() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let _unblock = Release(release.clone());
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
        ServerOptions {
            max_connections: core::num::NonZeroUsize::MIN,
            ..server_options()
        },
    )
    .await
    .unwrap();
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let (updates, results) = target(&client, 9).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();
    drop(results);
    timeout(Duration::from_secs(2), server.cancelled.notified())
        .await
        .unwrap();

    let next = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let reply = timeout(Duration::from_secs(2), read(&next).send())
        .await
        .unwrap()
        .unwrap();
    release.notify_one();
    running.await.unwrap();
    reply.await.unwrap();
    assert!(history(&actor).await.unwrap().is_empty());

    drop(updates);
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn budget() {
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
    let client = Client::<State>::new(
        Address::from(&socket),
        ClientOptions {
            timeout: Duration::from_millis(150),
            ..client_options()
        },
    )
    .unwrap();
    let (updates, mut results) = target(&client, 2).latest();
    results.next().await.unwrap().unwrap().unwrap();
    sleep(Duration::from_millis(200)).await;
    updates.send(3);
    results.next().await.unwrap().unwrap().unwrap();
    updates.send(1);
    let error = timeout(Duration::from_secs(2), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);
    release.notify_one();

    drop((updates, results));
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn invalid_retry_after_connect() {
    use aktor::call::RetryRequest;

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
    let (updates, mut results) = increment(&client, 1).latest();
    results.next().await.unwrap().unwrap().unwrap();

    let updates = updates.retry(AktorRetryOptions {
        attempts: core::num::NonZeroUsize::MIN,
        timeout: Duration::ZERO,
    });
    updates.send(10);
    let error = results.next().await.unwrap().unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::NotAdmitted);
    assert_eq!(read(&actor).await.unwrap().0, 1);

    drop((updates, results));
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn generations() {
    let directory = tempfile::tempdir().unwrap();
    for address in [
        Address::Unix(directory.path().join("power.sock")),
        Address::Tcp("127.0.0.1:0".parse().unwrap()),
        #[cfg(feature = "udp")]
        Address::Udp("127.0.0.1:0".parse().unwrap()),
    ] {
        generation(address).await;
    }
}

async fn generation(address: Address) {
    use aktor::dispatch::DataCodec;
    use aktor::worker::Codec;
    use wire::Message;

    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let _unblock = Release(release.clone());
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
    let mut old = client.inner.open().await.er_unwrap().stream;

    wire::write(
        &mut old,
        &Message::Latest {
            operation: target::NAME.into(),
            session: None,
            generation: 1,
        },
        wire::MAX_FRAME,
    )
    .await
    .er_unwrap();
    let Message::LatestReady { session } = wire::read(&mut old, wire::MAX_FRAME).await.er_unwrap()
    else {
        panic!("missing latest acknowledgement")
    };
    for revision in 1..=2 {
        wire::write(
            &mut old,
            &Message::Update {
                session,
                revision,
                input: <DataCodec as Codec<(u32,)>>::encode(&(revision as u32,))
                    .unwrap()
                    .into(),
            },
            wire::MAX_FRAME,
        )
        .await
        .er_unwrap();
        let _ = timeout(
            Duration::from_secs(2),
            wire::read(&mut old, wire::MAX_FRAME),
        )
        .await
        .unwrap()
        .er_unwrap();
        if revision == 1 {
            timeout(Duration::from_secs(2), entered.notified())
                .await
                .unwrap();
        }
    }

    let mut replacement = timeout(Duration::from_secs(2), client.inner.open())
        .await
        .unwrap()
        .er_unwrap()
        .stream;
    wire::write(
        &mut replacement,
        &Message::Latest {
            operation: target::NAME.into(),
            session: Some(session),
            generation: 2,
        },
        wire::MAX_FRAME,
    )
    .await
    .er_unwrap();
    let ready = timeout(
        Duration::from_secs(2),
        wire::read(&mut replacement, wire::MAX_FRAME),
    )
    .await
    .unwrap()
    .er_unwrap();
    assert!(matches!(ready, Message::LatestReady { session: ready } if ready == session));
    wire::write(
        &mut replacement,
        &Message::Update {
            session,
            revision: 3,
            input: <DataCodec as Codec<(u32,)>>::encode(&(3,)).unwrap().into(),
        },
        wire::MAX_FRAME,
    )
    .await
    .er_unwrap();
    let _ = timeout(
        Duration::from_secs(2),
        wire::read(&mut replacement, wire::MAX_FRAME),
    )
    .await
    .unwrap()
    .er_unwrap();
    release.notify_one();
    let _ = timeout(
        Duration::from_secs(2),
        wire::read(&mut replacement, wire::MAX_FRAME),
    )
    .await
    .unwrap()
    .er_unwrap();
    assert_eq!(history(&actor).await.unwrap(), [1, 3]);

    drop(old);
    connection::finish(replacement, Duration::from_secs(1));
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn lost() {
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
            let (_updates, mut results) = increment(&client, 1)
                .retry(AktorRetryOptions {
                    attempts: core::num::NonZeroUsize::new(2).unwrap(),
                    timeout: Duration::from_secs(2),
                })
                .latest();
            assert_eq!(
                timeout(Duration::from_secs(2), results.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                2
            );
        } else {
            let (_updates, mut results) = increment(&client, 1).latest();
            let error = timeout(Duration::from_secs(2), results.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);
            assert_eq!(read(&actor).await.unwrap().0, 1);
        }

        forwarding.abort();
        let _ = forwarding.await;
        server.shutdown().await.unwrap();
        let _ = actors.stop().await;
    }
}

#[tokio::test]
async fn uncertain_retry() {
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
    let client = Client::<State>::new(
        Address::from(&proxy),
        ClientOptions {
            retry_delay: Duration::from_millis(200),
            ..client_options()
        },
    )
    .unwrap();
    let (_updates, mut results) = echo(&client, vec![0; 1024])
        .retry(AktorRetryOptions {
            attempts: core::num::NonZeroUsize::new(2).unwrap(),
            timeout: Duration::from_secs(2),
        })
        .latest();
    timeout(Duration::from_secs(2), async {
        while read(&actor).await.unwrap().0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.shutdown().await.unwrap();
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

    let error = timeout(Duration::from_secs(3), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::OutcomeUnknown);

    forwarding.abort();
    let _ = forwarding.await;
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn backoff() {
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
    let proxy = directory.path().join("proxy.sock");
    let listener = UnixListener::bind(&proxy).unwrap();
    let client = Client::<State>::new(Address::from(&proxy), client_options()).unwrap();
    let (updates, mut results) = read(&client)
        .retry(AktorRetryOptions {
            attempts: core::num::NonZeroUsize::new(2).unwrap(),
            timeout: Duration::from_secs(5),
        })
        .latest();
    let forwarding = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        drop(first);
        let (mut client, mut backend) = proxy_pair(&listener, &socket).await;
        tokio::io::copy_bidirectional(&mut client, &mut backend)
            .await
            .unwrap();
    });
    timeout(Duration::from_secs(2), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    drop((updates, results));
    forwarding.await.unwrap();
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn obsolete() {
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
    let target_socket = socket.clone();
    let held = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let captured = held.clone();
    let resumed = release.clone();
    let forwarding = tokio::spawn(async move {
        let (client, backend) = proxy_pair(&listener, &target_socket).await;
        let (mut incoming, mut replies) = client.into_split();
        let (mut answers, mut requests) = backend.into_split();
        let reading = async {
            tokio::io::copy(&mut incoming, &mut requests).await.unwrap();
        };
        let writing = async {
            let mut first = true;
            loop {
                let answer = wire::read(&mut answers, wire::MAX_FRAME).await.er_unwrap();
                if first && matches!(answer, wire::Message::LatestAnswer { .. }) {
                    first = false;
                    captured.notify_one();
                    resumed.notified().await;
                }
                wire::write(&mut replies, &answer, wire::MAX_FRAME)
                    .await
                    .er_unwrap();
            }
        };
        tokio::join!(reading, writing);
    });
    let client = Client::<State>::new(Address::from(&proxy), client_options()).unwrap();
    let (updates, mut results) = target(&client, 7).latest();
    timeout(Duration::from_secs(2), held.notified())
        .await
        .unwrap();
    updates.send(8);
    release.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(2), results.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap(),
        8
    );

    updates.send(9);
    timeout(Duration::from_secs(2), async {
        while !results.buffered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    updates.send(10);
    assert_eq!(results.next().await.unwrap().unwrap().unwrap(), 10);

    drop((updates, results));
    server.shutdown().await.unwrap();
    forwarding.abort();
    let _ = forwarding.await;
    let _ = actors.stop().await;
}

#[tokio::test]
async fn actor_shutdown() {
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
    let (_updates, mut results) = target(&client, 9).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();

    let closing = actors.stop();
    let handle = actor.handle();
    timeout(Duration::from_secs(2), handle.wait_closing())
        .await
        .unwrap();
    release.notify_one();
    running.await.unwrap();

    let (_, output) = timeout(Duration::from_secs(2), async {
        tokio::join!(closing, results.next())
    })
    .await
    .unwrap();
    assert_eq!(output.unwrap().unwrap().unwrap(), 9);
    assert!(results.next().await.is_none());
    let _ = server.shutdown().await;
}

#[tokio::test]
async fn shutdown() {
    for overdue in [false, true] {
        let (actors, actor) = actor().await;
        let (entered, release) = gates(&actor).await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("power.sock");
        let server = Server::listen(
            listen_address(Address::from(&socket)),
            &actor,
            ServerOptions {
                shutdown_timeout: if overdue {
                    Duration::from_millis(30)
                } else {
                    Duration::from_secs(1)
                },
                ..server_options()
            },
        )
        .await
        .unwrap();
        let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
        let (updates, mut results) = slow(&client).latest();
        timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let closing = server.shutdown();
        tokio::pin!(closing);

        if overdue {
            assert!(
                failures(closing.await.unwrap_err().0).contains(&ServerFailure::ShutdownTimeout)
            );
            assert_eq!(
                results.next().await.unwrap().unwrap_err().outcome,
                AktorDeliveryOutcome::OutcomeUnknown
            );
            release.notify_one();
        } else {
            tokio::select! {
                _ = sleep(Duration::from_millis(20)) => release.notify_one(),
                _ = &mut closing => panic!("listener finished before accepted work"),
            }
            closing.await.unwrap();
            assert_eq!(results.next().await.unwrap().unwrap(), 999);
        }

        drop((updates, results));
        let _ = actors.stop().await;
    }
}

#[tokio::test]
async fn runtime() {
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
    let (updates, mut results) = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
            target(&client, 2).latest()
        })
    })
    .await
    .unwrap();

    let error = timeout(Duration::from_secs(2), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.report.contains("Closed"));

    drop(updates);
    server.shutdown().await.unwrap();
    let _ = actors.stop().await;
}

#[tokio::test]
async fn rejected() {
    let (actors, actor) = actor().await;
    let (entered, release) = gates(&actor).await.unwrap();
    let _unblock = Release(release.clone());
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
        ServerOptions {
            max_connections: core::num::NonZeroUsize::MIN,
            max_frame: 4096,
            ..server_options()
        },
    )
    .await
    .unwrap();
    let client = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let (updates, mut results) = echo(&client, vec![1]).latest();
    timeout(Duration::from_secs(2), results.seen(1))
        .await
        .unwrap();
    let overflow = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let error = increment(&overflow, 100).await.unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::NotAdmitted);
    let (refused, mut refusal) = increment(&overflow, 100).latest();
    let error = refusal.next().await.unwrap().unwrap_err();
    assert_eq!(error.outcome, AktorDeliveryOutcome::NotAdmitted);
    drop((refused, refusal));

    updates.send(vec![2]);
    timeout(Duration::from_secs(2), results.seen(2))
        .await
        .unwrap();

    updates.send(vec![0; 8192]);
    assert!(
        results
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .report
            .contains("Codec")
    );

    timeout(Duration::from_secs(2), server.cancelled.notified())
        .await
        .unwrap();
    let next = Client::<State>::new(Address::from(&socket), client_options()).unwrap();
    let reply = timeout(Duration::from_secs(2), read(&next).send())
        .await
        .unwrap()
        .unwrap();
    release.notify_one();
    running.await.unwrap();
    assert_eq!(reply.await.unwrap().0, 1);

    drop((updates, results));
    let (error, _) = server.shutdown().await.unwrap_err();
    assert_eq!(
        failures(error),
        vec![ServerFailure::session(Cause::Closed); 2]
    );
    let _ = actors.stop().await;
}
