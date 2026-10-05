use std::{hint::black_box, sync::Arc};
use tokio::sync::Notify;

#[derive(Clone, Copy)]
pub enum Work {
    Count,
    Cpu(usize),
    Yield,
}
impl Work {
    pub fn name(self) -> String {
        match self {
            Self::Count => "count".into(),
            Self::Cpu(rounds) => format!("cpu{rounds}"),
            Self::Yield => "yield".into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Counter {
    pub count: u64,
    pub closed: Option<Arc<Notify>>,
}
impl Counter {
    pub async fn step(&mut self, work: Work) -> u64 {
        if let Work::Yield = work {
            tokio::task::yield_now().await;
        }

        self.count += 1;

        let mut value = black_box(self.count);

        if let Work::Cpu(rounds) = work {
            for _ in 0..rounds {
                value = value.wrapping_mul(6364136223846793005).wrapping_add(1);
                value ^= value >> 17;
            }
        }

        black_box(value);
        self.count
    }
}
impl Drop for Counter {
    fn drop(&mut self) {
        if let Some(closed) = &self.closed {
            closed.notify_one();
        }
    }
}

#[aktor::aktor]
pub async fn step(counter: &mut Counter, work: Work) -> u64 {
    counter.step(work).await
}
