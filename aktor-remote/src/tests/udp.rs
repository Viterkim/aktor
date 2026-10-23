use super::*;
use tokio::net::UdpSocket;

#[tokio::test]
async fn packets() {
    let (actors, actor) = actor().await;
    let server = listen(Address::Udp("127.0.0.1:0".parse().unwrap()), &actor)
        .await
        .unwrap();
    let Address::Udp(address) = server.address else {
        panic!("not a UDP listener")
    };
    let front = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let backend = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    backend.connect(address).await.unwrap();
    let client = Client::<State>::new(Address::Udp(front.local_addr().unwrap()));
    let relay = tokio::spawn(async move {
        let mut incoming = [0; 65536];
        let mut outgoing = [0; 65536];
        let mut peer = None;
        let mut held = None::<Vec<u8>>;
        let mut dropped = false;
        let mut tick = tokio::time::interval(Duration::from_millis(2));
        loop {
            tokio::select! {
                packet = front.recv_from(&mut incoming) => {
                    let (length, source) = packet.unwrap();
                    peer = Some(source);
                    let bytes = &incoming[..length];
                    if bytes.first().is_some_and(|byte| byte >> 4 == 0) {
                        if !dropped {
                            dropped = true;
                            continue;
                        }
                        if let Some(previous) = held.take() {
                            backend.send(bytes).await.unwrap();
                            backend.send(bytes).await.unwrap();
                            backend.send(&previous).await.unwrap();
                            backend.send(&previous).await.unwrap();
                        } else {
                            held = Some(bytes.to_vec());
                        }
                    } else {
                        backend.send(bytes).await.unwrap();
                    }
                }
                packet = backend.recv(&mut outgoing) => {
                    let length = packet.unwrap();
                    if let Some(peer) = peer {
                        front.send_to(&outgoing[..length], peer).await.unwrap();
                    }
                }
                _ = tick.tick() => {
                    if let Some(bytes) = held.take() {
                        backend.send(&bytes).await.unwrap();
                        backend.send(&bytes).await.unwrap();
                    }
                }
            }
        }
    });

    let bytes = vec![17; 32768];
    let result = tokio::time::timeout(Duration::from_secs(5), echo(&client, bytes.clone()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, bytes);
    assert_eq!(read(&actor).await.0, 1);

    relay.abort();
    let _ = relay.await;
    server.shutdown().await.unwrap();
    actors.shutdown().await;
}
