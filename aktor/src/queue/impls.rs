use super::*;
use crate::message::{AktorStateError, AktorStateResult};
use crate::{AktorJob, AktorJobOutput, AktorOwnerTask, AktorSpawnError};
use core::num::NonZeroUsize;
use er::*;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

pub fn channel<M>(capacity: NonZeroUsize) -> AktorStateResult<(Arc<HandleInner<M>>, Mailbox<M>)> {
    if capacity.get() > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(AktorStateError::CapacityTooLarge);
    }

    Ok(build(capacity.get()))
}

/// One slot for a handle whose actor never launched, the caller closes it right away.
pub fn closed_channel<M>() -> (Arc<HandleInner<M>>, Mailbox<M>) {
    build(1)
}

fn build<M>(capacity: usize) -> (Arc<HandleInner<M>>, Mailbox<M>) {
    let admission = Arc::new(Admission::new(capacity));
    let (sender, receiver) = mailbox::channel(admission.permits.clone());
    let (finished, waiting) = watch::channel(false);

    let jobs = Arc::new(crate::owner_tasks::External::default());
    let handle = Arc::new(HandleInner {
        jobs: Arc::downgrade(&jobs),
        sender,
        finished: waiting,
        admission: admission.clone(),
    });

    (
        handle,
        Mailbox {
            jobs,
            receiver,
            admission,
            finished,
        },
    )
}

impl<M> HandleInner<M> {
    #[track_caller]
    pub(crate) fn spawn<O, E>(
        &self,
        work: impl Future<Output = O> + Send + 'static,
        policy: AktorJob,
    ) -> Result<AktorOwnerTask<O::Value>, (AktorSpawnError, String)>
    where
        O: AktorJobOutput<E> + 'static,
        O::Value: Send + 'static,
    {
        let jobs = self.jobs.upgrade().er(|| AktorSpawnError::Closing)?;
        let owner = jobs.owner.get().er(|| AktorSpawnError::Closing)?;
        jobs.spawn(&self.admission, owner.clone(), work, policy)
    }

    /// Finish what's queued, then stop.
    pub fn request_close(&self) {
        self.admission.close(|| self.sender.nudge());
    }

    /// Stop now, what's still queued gets discarded.
    pub fn retire(&self) {
        self.admission.close(|| {
            self.sender.retire();
            self.sender.nudge();
        });
    }
}

impl Admission {
    #[cfg(not(target_family = "wasm"))]
    pub fn set_standard(&self, standard: bool) {
        self.standard
            .store(standard, std::sync::atomic::Ordering::Release);
    }

    #[cfg(any(feature = "tokio", not(target_family = "wasm")))]
    pub fn is_standard(&self) -> bool {
        self.standard.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn new(capacity: usize) -> Self {
        Self {
            lost: Default::default(),
            terminal: Default::default(),
            job_capacity: capacity,
            permits: Arc::new(tokio::sync::Semaphore::new(capacity)),
            #[cfg(any(feature = "tokio", not(target_family = "wasm")))]
            standard: std::sync::atomic::AtomicBool::new(false),
            state: ReentrantMutex::new(RefCell::new(AdmissionState {
                group: None,
                phase: Phase::Running,
                refused: None,
                sessions: Vec::new(),
                prune_at: 64,
                callbacks: Default::default(),
                jobs: Default::default(),
                next_callback: 0,
            })),
            changed: watch::channel(()).0,
        }
    }

    pub fn manage(&self, group: crate::group::AktorKillSwitch) {
        self.state.lock().borrow_mut().group = Some(group);
    }

    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    pub fn group(&self) -> Option<crate::group::AktorKillSwitch> {
        self.state.lock().borrow().group.as_ref().cloned()
    }

    pub fn watch(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    pub fn phase(&self) -> Phase {
        self.state.lock().borrow().phase
    }

    pub fn admit<M>(&self, permit: mailbox::Permit<'_, M>, message: M) -> Result<(), M> {
        let lock = self.state.lock();

        if lock.borrow().phase.is_closing() {
            drop(lock);
            drop(permit);
            return Err(message);
        }

        let result = permit.commit(message);

        drop(lock);

        match result {
            Ok(ready) => {
                ready.wake();
                Ok(())
            }
            Err((permit, message)) => {
                drop(permit);
                Err(message)
            }
        }
    }

    pub(crate) fn call_error(
        &self,
        outcome: crate::AktorDeliveryOutcome,
        receive: bool,
    ) -> crate::AktorDeliveryError {
        let receipt = self.state.lock().borrow().refused.clone();
        let receipt = receipt.as_ref();
        if receive {
            crate::Delivery::error(outcome, receipt)
        } else {
            crate::Delivery::reported(outcome, receipt)
        }
    }

    pub(crate) fn admit_job(
        self: &Arc<Self>,
        job: &Arc<crate::owner_tasks::Shared>,
        push: impl FnOnce(JobPermit),
    ) -> Result<(), (AktorSpawnError, String)> {
        let lock = self.state.lock();
        let mut state = lock.borrow_mut();
        if state.phase.is_closing() || self.permits.is_closed() {
            er_bail!(AktorSpawnError::Closing);
        }
        if state.jobs.len() == self.job_capacity {
            er_bail!(AktorSpawnError::Full);
        }

        let id = Arc::as_ptr(job) as usize;
        state.jobs.insert(id, job.clone());
        drop(state);
        push(JobPermit {
            admission: self.clone(),
            id,
        });
        Ok(())
    }

    pub(crate) fn cut_jobs(&self, owner: &crate::owner_tasks::JobOwner) {
        let jobs = self.jobs(AktorJob::FinishBeforeEnd);
        for job in jobs {
            job.cut(owner);
        }
    }

    fn jobs(&self, policy: AktorJob) -> Vec<Arc<crate::owner_tasks::Shared>> {
        self.state
            .lock()
            .borrow()
            .jobs
            .values()
            .filter(|job| job.policy == policy)
            .cloned()
            .collect()
    }

    pub(crate) fn on_stop(&self, callback: Box<dyn FnOnce() + Send>) -> usize {
        {
            let lock = self.state.lock();
            let mut state = lock.borrow_mut();
            if !state.phase.is_closing() {
                let id = state.next_callback;
                state.next_callback += 1;
                state.callbacks.insert(id, callback);
                return id;
            }
        }
        callback();
        usize::MAX
    }

    pub(crate) fn unregister(&self, id: usize) {
        let callback = self.state.lock().borrow_mut().callbacks.remove(&id);
        drop(callback);
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
        self.close(|| {});
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.permits.is_closed()
    }

    pub(crate) fn close(&self, nudge: impl FnOnce()) {
        let previous = {
            let lock = self.state.lock();
            let mut state = lock.borrow_mut();
            if !state.phase.is_closing() {
                state.refused = self.lost.get().cloned();
            }
            core::mem::replace(&mut state.phase, Phase::Closing)
        };
        if previous.is_closing() {
            nudge();
            return;
        }

        let mut first = None;
        notify(&mut first, || self.permits.close());
        notify(&mut first, nudge);
        notify(&mut first, || self.changed.send_replace(()));
        for job in self.jobs(AktorJob::DropAtStop) {
            notify(&mut first, || job.stop());
        }
        loop {
            let stop = self.state.lock().borrow_mut().callbacks.pop_first();
            match stop {
                Some((_, stop)) => stop(),
                None => break,
            }
        }

        let callbacks: Vec<_> = {
            let lock = self.state.lock();
            let mut state = lock.borrow_mut();

            state.sessions.retain(|session| session.strong_count() != 0);
            state.sessions.iter().filter_map(Weak::upgrade).collect()
        };

        for callback in callbacks {
            notify(&mut first, || {
                callback(Phase::Closing);
            });
        }

        if let Some(payload) = first {
            resume_unwind(payload);
        }
    }
}

fn notify(first: &mut Option<Box<dyn std::any::Any + Send>>, action: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(action)) {
        match first {
            Some(_) => crate::panic::dispose_secondary(payload),
            None => *first = Some(payload),
        }
    }
}

impl Phase {
    pub fn is_closing(self) -> bool {
        match self {
            Phase::Running => false,
            Phase::Closing => true,
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

        let (inner, mut mailbox) = crate::queue::channel(NonZeroUsize::MIN).unwrap();
        let handle = crate::listener::Handle::<usize>::new(inner);
        let waker = Waker::from(Arc::new(AdmissionWake(handle.inner.admission.clone())));
        let mut receiver = Box::pin(mailbox.receiver.recv());

        assert!(
            receiver
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        crate::message::call(&handle, |_, ()| (), ())
            .cast()
            .await
            .unwrap();
        drop(receiver);
        mailbox.receiver.try_recv().unwrap().run(&mut 0).await;
    }

    #[test]
    fn live_sessions() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let admission = Admission::new(1);
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
        let admission = Admission::new(1);

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
        let admission = Arc::new(Admission::new(1));
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
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_close = closed.clone();

        let registration = std::thread::spawn(move || {
            registering.register_session(Arc::new(move |phase| {
                if !phase.is_closing() {
                    callback_entered.wait();
                    callback_resume.wait();
                }

                let current = observed.upgrade().unwrap().phase();

                saw_close.store(current.is_closing(), std::sync::atomic::Ordering::SeqCst);
                true
            }));
        });

        entered.wait();
        admission.shutdown();
        resume.wait();
        registration.join().unwrap();
        assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
    }
}
