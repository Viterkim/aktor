use super::*;

impl<S> Permit<'_, S> {
    pub fn submit(self, message: Message<S>) -> Result<(), Message<S>> {
        let mut entries = self.queue.entries.lock();
        if self.is_closed() {
            return Err(message);
        }
        entries.messages.push_back(message);
        self.permit.forget();
        drop(entries);
        self.queue.ready.notify_one();
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.queue.permits.is_closed()
    }
}

impl<S> Sender<S> {
    pub fn service(&self, message: Message<S>) -> Result<(), Message<S>> {
        let mut entries = self.0.entries.lock();
        if !entries.receiving {
            return Err(message);
        }
        entries.messages.push_back(message);
        drop(entries);
        self.0.ready.notify_one();
        Ok(())
    }
    pub async fn reserve(&self) -> Result<Permit<'_, S>, tokio::sync::AcquireError> {
        tokio::task::coop::consume_budget().await;
        let permit = self.0.permits.acquire().await?;

        Ok(Permit {
            queue: &self.0,
            permit,
        })
    }

    pub fn try_reserve(&self) -> Result<Permit<'_, S>, mpsc::error::TrySendError<()>> {
        let permit = self.0.permits.try_acquire().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => mpsc::error::TrySendError::Closed(()),
            tokio::sync::TryAcquireError::NoPermits => mpsc::error::TrySendError::Full(()),
        })?;

        Ok(Permit {
            queue: &self.0,
            permit,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.0.permits.is_closed()
    }

    pub fn capacity(&self) -> usize {
        self.0.permits.available_permits()
    }
}
impl<S> Drop for Sender<S> {
    fn drop(&mut self) {
        self.0.senders_closed.store(true, Ordering::Release);
        self.0.ready.notify_one();
    }
}

impl<S> Receiver<S> {
    pub async fn recv(&mut self) -> Option<Message<S>> {
        tokio::task::coop::consume_budget().await;

        loop {
            let ready = self.0.ready.notified();

            match self.try_recv() {
                Ok(message) => return Some(message),
                Err(mpsc::error::TryRecvError::Disconnected) => return None,
                Err(mpsc::error::TryRecvError::Empty) => ready.await,
            }
        }
    }

    pub fn try_recv(&self) -> Result<Message<S>, mpsc::error::TryRecvError> {
        let entry = {
            let mut entries = self.0.entries.lock();
            entries.messages.pop_front()
        };

        if let Some(entry) = entry {
            if entry.counted() {
                self.0.permits.add_permits(1);
            }
            return Ok(entry);
        }

        if self.0.permits.is_closed() || self.0.senders_closed.load(Ordering::Acquire) {
            Err(mpsc::error::TryRecvError::Disconnected)
        } else {
            Err(mpsc::error::TryRecvError::Empty)
        }
    }

    pub fn close(&mut self) {
        self.0.permits.close();
        self.0.ready.notify_one();
    }

    pub fn capacity(&self) -> usize {
        self.0.permits.available_permits()
    }

    pub fn len(&self) -> usize {
        self.0.entries.lock().messages.len()
    }
}
impl<S> Drop for Receiver<S> {
    fn drop(&mut self) {
        self.close();

        let messages = {
            let mut entries = self.0.entries.lock();
            entries.receiving = false;
            std::mem::take(&mut entries.messages)
        };
        drop(messages);
    }
}

pub fn channel<S>(capacity: usize) -> (Sender<S>, Receiver<S>) {
    let queue = Arc::new(Queue {
        entries: Mutex::new(Entries {
            receiving: true,
            messages: VecDeque::new(),
        }),
        permits: Semaphore::new(capacity),
        ready: Notify::new(),
        senders_closed: AtomicBool::new(false),
    });

    (Sender(queue.clone()), Receiver(queue))
}
