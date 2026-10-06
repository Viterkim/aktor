use super::*;
use crate::group::shutdown::{contain, contain_drop};
use std::{collections::VecDeque, sync::Mutex as SlotMutex};
use tokio::sync::Notify;

struct Queue<S> {
    pending: VecDeque<Message<S>>,
    closed: bool,
}

pub struct Services<S> {
    queue: SlotMutex<Queue<S>>,
    pub wake: Notify,
}
impl<S> Services<S> {
    pub fn new() -> Self {
        Self {
            queue: SlotMutex::new(Queue {
                pending: VecDeque::new(),
                closed: false,
            }),
            wake: Notify::new(),
        }
    }

    pub fn push(&self, message: Message<S>, draining: bool) -> Result<(), Message<S>> {
        let mut queue = self.queue.lock().unwrap_or_else(|error| error.into_inner());

        if queue.closed && !draining {
            return Err(message);
        }

        queue.pending.push_back(message);
        Ok(())
    }

    pub fn close(&self) {
        self.queue
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closed = true;
        self.wake.notify_one();
    }

    pub fn discard(&self) -> VecDeque<Message<S>> {
        let mut queue = self.queue.lock().unwrap_or_else(|error| error.into_inner());
        queue.closed = true;
        std::mem::take(&mut queue.pending)
    }

    pub async fn next(&self) -> Option<Message<S>> {
        loop {
            let changed = self.wake.notified();

            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut queue = self.queue.lock().unwrap_or_else(|error| error.into_inner());

                if let Some(message) = queue.pending.pop_front() {
                    return Some(message);
                }

                if queue.closed {
                    return None;
                }
            }
            changed.await;
        }
    }
}

pub struct ServiceDriver<S> {
    pub services: Arc<Services<S>>,
    pub kill: KillSwitch,
}
impl<S> Drop for ServiceDriver<S> {
    fn drop(&mut self) {
        let pending = self.services.discard();

        for message in pending {
            contain_drop(message, &self.kill, "task latest queue drop");
        }

        self.services.wake.notify_one();
    }
}

pub struct CallDriver<S> {
    pub receiver: mpsc::Receiver<Message<S>>,
    pub kill: KillSwitch,
}
impl<S> CallDriver<S> {
    pub fn close(&mut self) {
        contain(|| self.receiver.close(), &self.kill, "task queue close");
    }
}
impl<S> Drop for CallDriver<S> {
    fn drop(&mut self) {
        self.close();

        while let Ok(message) = self.receiver.try_recv() {
            contain_drop(message, &self.kill, "task queue drop");
        }
    }
}
