use super::*;
use crate::message::Message;

impl Admission {
    pub fn new() -> Self {
        Self {
            state: ReentrantMutex::new(RefCell::new(AdmissionState {
                open: true,
                epoch: 0,
            })),
            changed: watch::channel(0).0,
        }
    }

    pub fn watch(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
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
        mut message: Message<S>,
    ) -> Result<(), Message<S>> {
        loop {
            if self.epoch() != Some(epoch) || permit.is_closed() {
                return Err(message);
            }

            // Eq is user code. Don't call it while holding either lock.
            let latest = permit.find_latest(&message);
            let lock = self.state.lock();
            let valid = {
                let state = lock.borrow();
                state.open && state.epoch == epoch
            };

            if !valid {
                return Err(message);
            }

            match permit.submit(latest.as_ref(), message) {
                Ok(old) => {
                    drop(lock);
                    permit.finish(old.is_some(), latest.is_some());

                    if let Some(old) = old {
                        old.supersede();
                    }

                    return Ok(());
                }
                Err(returned) => {
                    drop(lock);
                    message = returned;
                }
            }
        }
    }

    pub fn replace<S>(
        &self,
        epoch: u64,
        sender: &mailbox::Sender<S>,
        mut message: Message<S>,
    ) -> Result<(), Message<S>> {
        loop {
            if self.epoch() != Some(epoch) {
                return Err(message);
            }

            // Key equality can submit work too. Compare before taking either lock.
            let Some(latest) = sender.find_latest(&message) else {
                return Err(message);
            };

            if latest.key.is_none() {
                return Err(message);
            }

            let lock = self.state.lock();
            let valid = {
                let state = lock.borrow();
                state.open && state.epoch == epoch
            };

            if !valid {
                return Err(message);
            }

            match sender.replace(&latest, message) {
                Ok(old) => {
                    drop(lock);
                    sender.notify();
                    old.supersede();
                    return Ok(());
                }
                Err(returned) => {
                    drop(lock);
                    message = returned;
                }
            }
        }
    }

    pub fn close(&self) {
        self.set(false);
    }

    pub fn open(&self) {
        self.set(true);
    }

    fn set(&self, open: bool) {
        let lock = self.state.lock();
        let mut state = lock.borrow_mut();

        if state.open == open {
            return;
        }

        state.open = open;
        state.epoch = state.epoch.wrapping_add(1);
        let epoch = state.epoch;
        drop(state);
        drop(lock);

        self.changed.send_replace(epoch);
    }
}
