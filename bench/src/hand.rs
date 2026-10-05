use crate::work::{Counter, Work};
use tokio::sync::{mpsc, oneshot};

pub struct Call {
    pub work: Work,
    pub reply: oneshot::Sender<u64>,
}

pub async fn serve(mut queue: mpsc::Receiver<Call>) {
    let mut counter = Counter::default();

    while let Some(call) = queue.recv().await {
        let _ = call.reply.send(counter.step(call.work).await);
    }
}

pub async fn step(sender: &mpsc::Sender<Call>, work: Work) -> u64 {
    let (reply, answer) = oneshot::channel();
    sender.send(Call { work, reply }).await.unwrap();
    answer.await.unwrap()
}
