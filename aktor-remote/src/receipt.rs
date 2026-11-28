use crate::{
    connection::BufferedStream,
    wire::{self, Message},
};
use aktor::FailureId;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::time::timeout;

pub struct Confirmation {
    runtime: tokio::runtime::Handle,
    exchange: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl Confirmation {
    pub fn send(self) {
        self.runtime.spawn(self.exchange);
    }
}
pub type Pending = Mutex<Option<Confirmation>>;

pub fn confirm(
    mut stream: BufferedStream,
    id: FailureId,
    limit: usize,
    budget: Duration,
) -> Confirmation {
    let exchange = Box::pin(async move {
        let exchange = async {
            wire::write(&mut stream, &Message::DeliveryReceived(id), limit)
                .await
                .ok()?;
            wire::FrameReader::default()
                .read(&mut stream, limit)
                .await
                .ok()
        };
        // The caller already has the owner's report if the connection dies during confirmation.
        let _ = timeout(budget, exchange).await;
    });
    Confirmation {
        runtime: tokio::runtime::Handle::current(),
        exchange,
    }
}

pub fn listen(
    mut stream: BufferedStream,
    id: FailureId,
    limit: usize,
    budget: Duration,
    receive: impl FnOnce() + Send + 'static,
) {
    #[cfg(feature = "udp")]
    let pending = match stream.get_ref() {
        crate::connection::Stream::Udp(stream) => Some(stream.pending_receipt()),
        _ => None,
    };

    // A client can hold an unread reply after the actor and its listener have stopped.
    tokio::spawn(async move {
        #[cfg(feature = "udp")]
        let _pending = pending;

        let frames = wire::FrameReader::default();
        loop {
            match frames.read_idle(&mut stream, limit, budget).await {
                Ok(Some(Message::DeliveryReceived(received))) if received == id => break,
                Ok(Some(Message::Update { .. })) => continue,
                _ => return,
            }
        }
        receive();
        let _ = timeout(
            budget,
            wire::write(&mut stream, &Message::DeliveryReceived(id), limit),
        )
        .await;
    });
}

#[derive(Default)]
pub struct Receipt {
    pub pending: Arc<Pending>,
}

impl Receipt {
    pub fn poll<O>(
        &mut self,
        cx: &mut Context<'_>,
        answer: impl FnOnce(&mut Context<'_>) -> Poll<Result<O, aktor::AktorDeliveryError>>,
    ) -> Poll<Result<O, aktor::AktorDeliveryError>> {
        let result = ready!(answer(cx));
        if result.is_err()
            && let Some(confirmation) = self.pending.lock().unwrap().take()
        {
            confirmation.send();
        }
        Poll::Ready(result)
    }
}
