use super::*;
use crate::message::Message;

impl Admission {
    pub fn set_standard(&self, standard: bool) {
        self.standard
            .store(standard, std::sync::atomic::Ordering::Release);
    }

    pub fn is_standard(&self) -> bool {
        self.standard.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn new() -> Self {
        Self {
            standard: std::sync::atomic::AtomicBool::new(false),
            state: ReentrantMutex::new(RefCell::new(AdmissionState {
                open: true,
                epoch: 0,
                group: None,
                phase: Phase::Running,
                sessions: Vec::new(),
                prune_at: 64,
            })),
            changed: watch::channel(0).0,
        }
    }

    pub fn manage(&self, name: String, group: crate::group::KillSwitch) {
        self.state.lock().borrow_mut().group = Some((name, group));
    }

    pub fn group(&self) -> Option<crate::group::KillSwitch> {
        self.state
            .lock()
            .borrow()
            .group
            .as_ref()
            .map(|(_, group)| group.clone())
    }

    pub fn watch(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub fn phase(&self) -> Phase {
        self.state.lock().borrow().phase
    }

    pub fn epoch(&self) -> Option<u64> {
        let lock = self.state.lock();
        let state = lock.borrow();
        state.open.then_some(state.epoch)
    }

    pub fn admit<S>(
        &self,
        epoch: u64,
        permit: mailbox::Permit<'_, S>,
        message: Message<S>,
    ) -> Result<(), Message<S>> {
        let lock = self.state.lock();
        let valid = {
            let state = lock.borrow();
            state.open && state.epoch == epoch
        };

        if !valid {
            drop(lock);
            drop(permit);
            return Err(message);
        }

        let result = permit.commit(message);

        drop(lock);

        match result {
            Ok(ready) => {
                ready.notify_one();
                Ok(())
            }
            Err((permit, message)) => {
                drop(permit);
                Err(message)
            }
        }
    }

    pub fn lost(&self) {
        let group = self.state.lock().borrow().group.clone();

        if let Some((name, group)) = group {
            group.fail(crate::group::ActorFailure {
                kind: None,
                actor: name,
                phase: "call".into(),
                message: "actor stopped without returning the operation's output".into(),
            });
        }
    }

    pub fn rejected(&self) {
        if !self.group().is_some_and(|group| group.is_stopping()) {
            self.lost();
        }
    }

    pub fn close(&self) {
        self.set(|_| Phase::Paused);
    }

    pub fn open(&self) {
        self.set(|_| Phase::Running);
    }

    pub fn register_session(&self, callback: Arc<dyn Fn(Phase) -> bool + Send + Sync>) {
        let phase = {
            let lock = self.state.lock();
            let mut state = lock.borrow_mut();

            while state
                .sessions
                .last()
                .is_some_and(|session| session.strong_count() == 0)
            {
                state.sessions.pop();
            }

            if state.sessions.len() >= state.prune_at {
                state.sessions.retain(|session| session.strong_count() != 0);
                state.prune_at = state.sessions.len().saturating_mul(2).max(64);
            }

            state.sessions.push(Arc::downgrade(&callback));
            state.phase
        };

        callback(phase);
    }

    pub fn shutdown(&self) {
        self.set(|phase| Phase::Closing {
            paused: matches!(phase, Phase::Paused),
        });
    }

    fn set(&self, transition: impl FnOnce(Phase) -> Phase) {
        let lock = self.state.lock();
        let mut state = lock.borrow_mut();

        if matches!(state.phase, Phase::Closing { .. }) {
            return;
        }

        let phase = transition(state.phase);
        let open = matches!(phase, Phase::Running);

        state.phase = phase;
        state.open = open;
        state.epoch = state.epoch.wrapping_add(1);

        let epoch = state.epoch;

        drop(state);
        drop(lock);

        self.changed.send_replace(epoch);

        let callbacks: Vec<_> = {
            let lock = self.state.lock();
            let mut state = lock.borrow_mut();

            state.sessions.retain(|session| session.strong_count() != 0);
            state.sessions.iter().filter_map(Weak::upgrade).collect()
        };

        for callback in callbacks {
            callback(phase);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AdmissionWake(Arc<Admission>);
    impl std::task::Wake for AdmissionWake {
        fn wake(self: Arc<Self>) {
            let admission = self.0.clone();

            assert!(
                std::thread::spawn(move || admission.state.try_lock().is_some())
                    .join()
                    .unwrap(),
                "receiver woke under the admission lock"
            );
        }
    }

    #[tokio::test]
    async fn notification_releases_admission() {
        use core::{
            future::Future,
            task::{Context, Waker},
        };

        let (handle, mut listener) = crate::listener::channel::<usize>(1).unwrap();
        let waker = Waker::from(Arc::new(AdmissionWake(handle.inner.admission.clone())));
        let mut receiver = Box::pin(listener.recv());

        assert!(
            receiver
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        crate::message::call(&handle, |_, ()| (), ()).cast().await;
        drop(receiver);
        listener.try_recv().unwrap().run(&mut 0).await;
    }

    #[test]
    fn live_sessions() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let admission = Admission::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut sessions = Vec::new();

        for _ in 0..512 {
            let calls = calls.clone();
            let callback: Arc<dyn Fn(Phase) -> bool + Send + Sync> = Arc::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                true
            });

            admission.register_session(callback.clone());
            sessions.push(callback);
        }

        assert_eq!(calls.load(Ordering::Relaxed), 512);
    }

    #[test]
    fn session_churn() {
        let admission = Admission::new();

        for _ in 0..1000 {
            let session = Arc::new(());
            let weak = Arc::downgrade(&session);

            admission.register_session(Arc::new(move |_| weak.upgrade().is_some()));
            drop(session);
        }

        assert_eq!(admission.state.lock().borrow().sessions.len(), 1);
    }

    #[test]
    fn session_registration() {
        let admission = Arc::new(Admission::new());
        let observed = Arc::downgrade(&admission);

        admission.register_session(Arc::new(move |_| {
            let admission = observed.upgrade().unwrap();
            let other = admission.clone();

            assert!(
                std::thread::spawn(move || other.state.try_lock().is_some())
                    .join()
                    .unwrap()
            );
            true
        }));

        let entered = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        let registering = admission.clone();
        let callback_entered = entered.clone();
        let callback_resume = resume.clone();
        let observed = Arc::downgrade(&admission);
        let paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_pause = paused.clone();

        let registration = std::thread::spawn(move || {
            registering.register_session(Arc::new(move |phase| {
                if matches!(phase, Phase::Running) {
                    callback_entered.wait();
                    callback_resume.wait();
                }

                let current = observed.upgrade().unwrap().phase();

                saw_pause.store(
                    matches!(current, Phase::Paused),
                    std::sync::atomic::Ordering::SeqCst,
                );
                true
            }));
        });

        entered.wait();
        admission.close();
        resume.wait();
        registration.join().unwrap();
        assert!(paused.load(std::sync::atomic::Ordering::SeqCst));
    }
}
