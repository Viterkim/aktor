use super::*;

impl<S> Permit<'_, S> {
    pub fn find_latest(&self, message: &Message<S>) -> Option<Snapshot> {
        self.queue.find_latest(message)
    }

    pub fn submit(
        &self,
        latest: Option<&Snapshot>,
        message: Message<S>,
    ) -> Result<Option<Message<S>>, Message<S>> {
        let mut entries = self.queue.entries.lock();

        if self.is_closed() {
            return Err(message);
        }

        entries.submit(latest, message)
    }

    pub fn finish(self, replaced: bool, latest: bool) {
        if replaced {
            drop(self.permit);
        } else {
            self.permit.forget();
        }

        self.queue.ready.notify_one();
        if latest {
            self.queue.changed.send_replace(());
        }
    }

    pub fn is_closed(&self) -> bool {
        self.queue.permits.is_closed()
    }
}

impl<S> Entries<S> {
    fn submit(
        &mut self,
        snapshot: Option<&Snapshot>,
        message: Message<S>,
    ) -> Result<Option<Message<S>>, Message<S>> {
        // Someone queued or took latest work while Eq ran. Look again.
        if snapshot.is_some_and(|snapshot| snapshot.revision != self.revision) {
            return Err(message);
        }

        if let Some(key) = snapshot.and_then(|snapshot| snapshot.key.as_ref()) {
            return self.replace(key, message).map(Some);
        }

        self.push(message);
        Ok(None)
    }

    fn replace(&mut self, key: &LatestKey, message: Message<S>) -> Result<Message<S>, Message<S>> {
        let Some(index) = self
            .messages
            .iter()
            .position(|entry| entry.has_latest_key(key))
        else {
            return Err(message);
        };

        let Some(old) = self.messages.remove(index) else {
            return Err(message);
        };

        self.push(message);
        Ok(old)
    }

    fn push(&mut self, message: Message<S>) {
        if message.latest_key().is_some() {
            self.revision = self.revision.wrapping_add(1);
        }

        self.messages.push_back(message);
    }
}

impl<S> Queue<S> {
    fn find_latest(&self, message: &Message<S>) -> Option<Snapshot> {
        let key = message.latest_key()?;
        let (revision, candidates) = {
            let entries = self.entries.lock();
            let candidates: Vec<_> = entries
                .messages
                .iter()
                .filter_map(Message::latest_key)
                .collect();

            (entries.revision, candidates)
        };

        Some(Snapshot {
            revision,
            key: candidates
                .into_iter()
                .find(|candidate| candidate.same(&key)),
        })
    }
}

impl<S> Sender<S> {
    pub fn watch(&self) -> watch::Receiver<()> {
        self.0.changed.subscribe()
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

    pub fn find_latest(&self, message: &Message<S>) -> Option<Snapshot> {
        self.0.find_latest(message)
    }

    pub fn replace(
        &self,
        latest: &Snapshot,
        message: Message<S>,
    ) -> Result<Message<S>, Message<S>> {
        let mut entries = self.0.entries.lock();

        if self.is_closed() || entries.revision != latest.revision {
            return Err(message);
        }

        let Some(key) = &latest.key else {
            return Err(message);
        };

        entries.replace(key, message)
    }

    pub fn notify(&self) {
        self.0.ready.notify_one();
        self.0.changed.send_replace(());
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
            let entry = entries.messages.pop_front();

            if entry
                .as_ref()
                .is_some_and(|entry| entry.latest_key().is_some())
            {
                entries.revision = entries.revision.wrapping_add(1);
            }

            entry
        };

        if let Some(entry) = entry {
            self.0.permits.add_permits(1);
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

        let entries = std::mem::take(&mut self.0.entries.lock().messages);
        drop(entries);
    }
}

pub fn channel<S>(capacity: usize) -> (Sender<S>, Receiver<S>) {
    let queue = Arc::new(Queue {
        entries: Mutex::new(Entries {
            messages: VecDeque::new(),
            revision: 0,
        }),
        permits: Semaphore::new(capacity),
        ready: Notify::new(),
        changed: watch::channel(()).0,
        senders_closed: AtomicBool::new(false),
    });

    (Sender(queue.clone()), Receiver(queue))
}
