use super::*;
use crate::message::Message;

impl Admission {
    pub fn new() -> Self {
        Self {
            state: ReentrantMutex::new(RefCell::new(AdmissionState {
                open: true,
                epoch: 0,
                group: None,
                phase: Phase::Running,
                sessions: Vec::new(),
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
            return Err(message);
        }
        permit.submit(message)
    }

    pub fn lost(&self) {
        if let Some((name, group)) = &self.state.lock().borrow().group {
            group.fail(crate::group::ActorFailure {
                actor: name.clone(),
                phase: "call".into(),
                message: "actor stopped without returning the operation's output".into(),
            });
        }
    }

    pub fn close(&self) {
        self.set(Phase::Paused);
    }

    pub fn open(&self) {
        self.set(Phase::Running);
    }

    pub fn register_session(&self, callback: Box<dyn Fn(Phase) -> bool + Send + Sync>) {
        let (phase, callbacks) = {
            let lock = self.state.lock();
            let mut state = lock.borrow_mut();
            state.sessions.push(Arc::from(callback));
            (state.phase, state.sessions.clone())
        };
        let expired: Vec<_> = callbacks.into_iter().filter(|live| !live(phase)).collect();
        self.state
            .lock()
            .borrow_mut()
            .sessions
            .retain(|live| !expired.iter().any(|dead| Arc::ptr_eq(live, dead)));
    }

    pub fn shutdown(&self) {
        let paused = matches!(self.state.lock().borrow().phase, Phase::Paused);
        self.set(Phase::Closing { paused });
    }

    fn set(&self, phase: Phase) {
        let open = matches!(phase, Phase::Running);
        let lock = self.state.lock();
        let mut state = lock.borrow_mut();
        if matches!(state.phase, Phase::Closing { .. }) {
            return;
        }
        state.phase = phase;
        state.open = open;
        state.epoch = state.epoch.wrapping_add(1);
        let epoch = state.epoch;
        drop(state);
        drop(lock);

        self.changed.send_replace(epoch);
        let callbacks = self.state.lock().borrow().sessions.clone();
        let expired: Vec<_> = callbacks
            .into_iter()
            .filter(|callback| !callback(phase))
            .collect();
        self.state
            .lock()
            .borrow_mut()
            .sessions
            .retain(|callback| !expired.iter().any(|dead| Arc::ptr_eq(callback, dead)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_churn() {
        let admission = Admission::new();
        for _ in 0..1000 {
            let session = Arc::new(());
            let weak = Arc::downgrade(&session);
            admission.register_session(Box::new(move |_| weak.upgrade().is_some()));
            drop(session);
        }
        assert_eq!(admission.state.lock().borrow().sessions.len(), 1);
    }

    #[test]
    fn session_registration() {
        let admission = Arc::new(Admission::new());
        let observed = Arc::downgrade(&admission);
        admission.register_session(Box::new(move |_| {
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
            registering.register_session(Box::new(move |phase| {
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
