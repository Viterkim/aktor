use super::*;
use crate::report::ShutdownCut;
use crate::{AktorExecution, AktorFailureInput, AktorPhase, FailureId};
#[cfg(feature = "std")]
use crate::{
    local::panic,
    panic::{dispose_with, panic_tree},
};
use core::num::NonZeroUsize;
use core::{
    cell::Cell,
    future::{Future, poll_fn},
    task::Poll,
};

/// The target's critical-section implementation must synchronize every calling core.
#[doc(hidden)]
pub fn channel<S>(capacity: NonZeroUsize) -> (Handle<S>, Owner<S>) {
    let (handle, inner) = build(capacity.get());
    (handle, Owner::new(inner))
}

#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_feature = "atomics"
))]
pub(crate) fn browser_channel<S>(
    capacity: NonZeroUsize,
) -> (Handle<S>, impl FnOnce() -> Owner<S> + Send) {
    let (handle, inner) = build(capacity.get());
    (handle, move || Owner::new(inner))
}

/// One slot for a handle whose actor never launched, the caller drops the owner right away.
pub(crate) fn closed_channel<S>(id: Option<crate::ActorId>) -> (Handle<S>, Owner<S>) {
    let (handle, inner) = build(1);
    inner
        .status
        .state
        .lock(|state| state.borrow_mut().actor_id = id);
    (handle, Owner::new(inner))
}

fn build<S>(capacity: usize) -> (Handle<S>, Arc<Inner<S>>) {
    let inner = Arc::new(Inner {
        queue: Mutex::new(RefCell::new(Queue {
            open: true,
            cancelled: false,
            handles: 1,
            ordinary: VecDeque::new(),
            services: VecDeque::new(),
        })),
        capacity,
        changed: Arc::new(Event::default()),
        status: Arc::new(Status {
            state: Mutex::new(RefCell::new(State {
                actor_id: None,
                returned: Vec::new(),
                lost: None,
                refused: None,
                cut_call: false,
                ran_out: false,
                init: crate::AktorInitEvidence::NeverEntered,
                cleanup_completed: false,
                managed: false,
                group_stopped: false,
                closing: false,
                finishing: false,
                ready: None,
                completed: None,
                diagnostics: Vec::new(),
                #[cfg(feature = "std")]
                pending_drops: Vec::new(),
                #[cfg(feature = "std")]
                phase: None,
                cleanup_failure: None,
            })),
            changed: Arc::new(Event::default()),
        }),
    });

    (
        Handle {
            inner: inner.clone(),
            role: PhantomData,
        },
        inner,
    )
}

impl<S> Owner<S> {
    fn new(inner: Arc<Inner<S>>) -> Self {
        Self {
            inner,
            hooks: CallHooks::default(),
            supervisor: None,
            local: PhantomData,
            intervals: Vec::new(),
            interval_cursor: 0,
            work_cursor: 0,
        }
    }
}

impl<S> Inner<S> {
    pub fn cancel_service(&self, id: *const ()) {
        let message = self.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();
            let index = queue
                .services
                .iter()
                .position(|message| message.job.service_id() == Some(id));
            index.and_then(|index| queue.services.remove(index))
        });

        drop(message);
    }

    pub fn close(&self) {
        let was_open = self.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();
            let was_open = queue.open;

            self.close_queue(&mut queue);
            was_open
        });

        // Only the close that closed wakes anyone, a second one would find the same waiters
        // and give a panicking waker another go at skipping the queue drop.
        if was_open {
            self.notify();
        }
    }

    pub fn cancel_calls(&self) {
        self.queue.lock(|queue| queue.borrow_mut().cancelled = true);
        self.notify();
    }

    fn is_cancelled(&self) -> bool {
        self.queue.lock(|queue| queue.borrow().cancelled)
    }

    fn mark_cut(&self) {
        self.status
            .state
            .lock(|state| state.borrow_mut().cut_call = true);
    }

    fn notify(&self) {
        #[cfg(feature = "std")]
        if let Some(payload) = panic::capture_each(
            [&self.changed, &self.status.changed],
            |event| event.notify(),
            |payload| self.status.secondary(payload),
        ) {
            std::panic::resume_unwind(payload);
        }
        #[cfg(not(feature = "std"))]
        {
            self.changed.notify();
            self.status.changed.notify();
        }
    }

    fn close_queue(&self, queue: &mut Queue<S>) {
        if queue.open {
            queue.open = false;
            self.status.state.lock(|state| {
                let mut state = state.borrow_mut();
                state.refused = state.lost.clone();
                state.closing = true;
            });
        }
    }

    fn discard_queued(&self) {
        self.close();
        let discarded = self.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();

            self.status
                .state
                .lock(|state| state.borrow_mut().finishing = true);
            (
                core::mem::take(&mut queue.ordinary),
                core::mem::take(&mut queue.services),
            )
        });

        let (ordinary, services) = discarded;
        #[cfg(feature = "std")]
        let primary = panic::discard(ordinary.into_iter().chain(services), |payload| {
            self.status.secondary(payload)
        });
        #[cfg(not(feature = "std"))]
        for message in ordinary.into_iter().chain(services) {
            drop(message);
        }
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }
    }

    pub fn finish(&self, result: Result<(), String>, delivery: Option<crate::Delivery>) {
        self.status
            .state
            .lock(|state| state.borrow_mut().returned.extend(delivery));
        // Closing wakes everyone who waits on this actor. A waker that panics here still
        // has to leave the queue dropped and the completion published.
        #[cfg(feature = "std")]
        let mut primary =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.close())).err();
        #[cfg(not(feature = "std"))]
        self.close();

        let mut completion = CompletionGuard {
            status: self.status.clone(),
            fallback: result.as_ref().err().cloned(),
            armed: true,
        };

        #[cfg(feature = "std")]
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.discard_queued()))
        {
            match primary {
                Some(_) => {
                    self.status.secondary(&payload);
                    dispose_with(payload, |nested| self.status.secondary(nested));
                }
                None => primary = Some(payload),
            }
        }
        #[cfg(not(feature = "std"))]
        self.discard_queued();

        #[cfg(feature = "std")]
        let result = if result.is_ok() && primary.is_some() {
            Err(completion.fallback.clone().unwrap_or_else(work_destroyed))
        } else {
            result
        };

        #[cfg(feature = "std")]
        if let Some(payload) = &primary {
            let error = panic_tree(payload).er_report();
            self.status
                .state
                .lock(|state| state.borrow_mut().diagnostics.push(error));
        }
        self.status.finish(result);
        completion.armed = false;
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }
    }

    pub fn commit(&self, message: Message<S>) -> Result<(), Refused<S>> {
        self.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();
            if !queue.open {
                return Err(Refused::Closed(message));
            }
            if queue.ordinary.len() == self.capacity {
                return Err(Refused::Full(message));
            }

            queue.ordinary.push_back(message);
            Ok(())
        })
    }

    pub fn commit_service(&self, message: Message<S>) -> Result<(), Message<S>> {
        self.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();
            if self.status.state.lock(|state| state.borrow().finishing) {
                return Err(message);
            }

            queue.services.push_back(message);
            Ok(())
        })
    }
}

impl Supervisor {
    fn fail(&self, phase: AktorPhase, report: &str) -> FailureId {
        let startup = phase == AktorPhase::Start;
        let failure = AktorFailureInput::actor(self.name.clone(), self.kind, phase, report.into());
        if startup {
            self.kill.fail_startup(failure)
        } else {
            self.kill.fail(failure)
        }
    }
}

impl Status {
    #[cfg(feature = "std")]
    fn secondary(&self, payload: &Box<dyn std::any::Any + Send>) {
        let report = panic_tree(payload).er_report();
        self.state.lock(|state| {
            let mut state = state.borrow_mut();
            if state.managed {
                state.pending_drops.push(report);
            } else {
                state.diagnostics.push(report);
            }
        });
    }

    pub fn finish(&self, result: Result<(), String>) {
        self.state.lock(|state| {
            let mut state = state.borrow_mut();

            if state.ready.is_none() {
                state.ready = Some(result.clone());
            }

            if state.completed.is_none() {
                state.completed = Some(result);
            }
        });

        self.changed.notify();
    }
}

impl<S, Role> Handle<S, Role> {
    pub fn actor_id(&self) -> Option<crate::ActorId> {
        self.inner
            .status
            .state
            .lock(|state| state.borrow().actor_id)
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    #[doc(hidden)]
    pub(crate) fn manage(&self, id: Option<crate::ActorId>) {
        self.inner.status.state.lock(|state| {
            let mut state = state.borrow_mut();
            state.managed = true;
            state.actor_id = id;
        });
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    #[doc(hidden)]
    pub fn finish_failed(&self, error: String) {
        if self
            .inner
            .status
            .state
            .lock(|state| state.borrow().completed.is_none())
        {
            self.inner.finish(Err(error), None);
        }
    }

    fn new_handle(&self) -> Self {
        self.inner
            .queue
            .lock(|queue| queue.borrow_mut().handles += 1);
        Self {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }

    pub fn with_role<R>(self) -> Handle<S, R> {
        self.inner
            .queue
            .lock(|queue| queue.borrow_mut().handles += 1);
        Handle {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }

    pub fn downgrade(&self) -> WeakHandle<S, Role> {
        WeakHandle {
            inner: Arc::downgrade(&self.inner),
            role: PhantomData,
        }
    }

    /// Non-owning supervision handle that does not increment the user handle count.
    pub(crate) fn control(&self) -> Control<S> {
        Control {
            inner: self.inner.clone(),
        }
    }

    #[doc(hidden)]
    pub fn shutdown(&self) -> Completion {
        self.inner.close();
        self.completion()
    }

    /// Stop serving calls, `end` still runs. Shutdown does this once an actor's grace is up.
    #[doc(hidden)]
    pub fn cancel_calls(&self) {
        self.inner.cancel_calls();
    }

    /// Whether cancelling cut calls that were still being served.
    #[doc(hidden)]
    pub fn cut_call(&self) -> bool {
        self.inner
            .status
            .state
            .lock(|state| state.borrow().cut_call)
    }

    #[doc(hidden)]
    pub fn completion(&self) -> Completion {
        Completion {
            status: self.inner.status.clone(),
        }
    }

    /// Wrap the outermost future that needs to stop with this actor's group.
    /// If the group starts closing first, `run` drops that future and returns `None`.
    pub fn run<F: IntoFuture>(&self, work: F) -> Run<F::IntoFuture> {
        let work = work.into_future();

        Run {
            status: self.inner.status.clone(),
            changed: Event::listen(&self.inner.status.changed),
            work,
        }
    }

    #[doc(hidden)]
    pub fn group_stopped(&self) {
        self.inner.status.state.lock(|state| {
            state.borrow_mut().group_stopped = true;
        });
        self.inner.status.changed.notify();
    }

    pub async fn ready(&self) -> Result<(), (OwnerError, String)> {
        let result = self.observe_ready().await;
        if result.is_err() {
            self.completion().receive();
        }
        result
    }

    pub(crate) async fn observe_ready(&self) -> Result<(), (OwnerError, String)> {
        let changed = Event::listen(&self.inner.status.changed);

        let ready = poll_fn(|cx| {
            changed.register(cx);
            self.inner
                .status
                .state
                .lock(|state| state.borrow().ready.clone())
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await;

        match ready {
            Ok(()) => Ok(()),
            Err(report) => Err(self.completion().report(OwnerError::Setup, report)),
        }
    }
}
impl<S, Role> Clone for Handle<S, Role> {
    fn clone(&self) -> Self {
        self.new_handle()
    }
}
impl<S, Role> Drop for Handle<S, Role> {
    fn drop(&mut self) {
        let last = self.inner.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();

            queue.handles -= 1;

            if queue.handles == 0 {
                self.inner.close_queue(&mut queue);
                true
            } else {
                false
            }
        });

        #[cfg(all(test, feature = "std"))]
        if last {
            let action = tests::LAST_HANDLE.with(|action| action.borrow_mut().take());

            if let Some(action) = action {
                action();
            }
        }

        if last {
            self.inner.notify();
        }
    }
}

impl<S, Role> WeakHandle<S, Role> {
    /// Tell every handle's `run` the group closed. Works after the queue closed too.
    #[doc(hidden)]
    pub fn group_stopped(&self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };

        inner.status.state.lock(|state| {
            state.borrow_mut().group_stopped = true;
        });
        inner.status.changed.notify();
    }

    pub fn upgrade(&self) -> Option<Handle<S, Role>> {
        let inner = self.inner.upgrade()?;
        let open = inner.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();

            if !queue.open {
                return false;
            }

            queue.handles += 1;
            true
        });

        if open {
            Some(Handle {
                inner,
                role: PhantomData,
            })
        } else {
            None
        }
    }
}
impl<S, Role> Clone for WeakHandle<S, Role> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            role: PhantomData,
        }
    }
}

impl Completion {
    #[cfg(feature = "embassy_cross_core")]
    pub(crate) fn owner_status(&self) -> crate::local::OwnerStatus {
        self.status.state.lock(|state| {
            let state = state.borrow();

            crate::local::OwnerStatus {
                cleanup_completed: Some(state.cleanup_completed),
                ran_past_grace: state.cut_call || state.ran_out,
                init: state.init,
            }
        })
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn initialized(&self) -> bool {
        self.status
            .state
            .lock(|state| state.borrow().ready.as_ref().is_some_and(Result::is_ok))
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn cleanup_completed(&self) -> bool {
        self.status
            .state
            .lock(|status| status.borrow().cleanup_completed)
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn cut_call(&self) -> bool {
        self.status.state.lock(|status| {
            let status = status.borrow();

            status.cut_call || status.ran_out
        })
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn ran_out(&self) -> bool {
        self.status.state.lock(|status| status.borrow().ran_out)
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn cleanup_failed(&self) -> bool {
        self.status
            .state
            .lock(|state| state.borrow().cleanup_failure.is_some())
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn init(&self) -> crate::AktorInitEvidence {
        self.status.state.lock(|s| s.borrow().init)
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn interrupted_phase(&self) -> AktorPhase {
        self.status.state.lock(|s| {
            s.borrow()
                .phase
                .as_ref()
                .map(Activity::phase)
                .unwrap_or(AktorPhase::Join)
        })
    }

    pub async fn wait(&self) -> Result<(), (OwnerError, String)> {
        let changed = Event::listen(&self.status.changed);

        let result = poll_fn(|cx| {
            changed.register(cx);
            self.status
                .state
                .lock(|state| state.borrow().completed.clone())
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await;

        if let Err(report) = result {
            let error = self.report(OwnerError::Completion, report);
            self.receive();
            return Err(error);
        }

        Ok(())
    }

    fn receive(&self) {
        self.status.state.lock(|state| {
            for receipt in &state.borrow().returned {
                receipt.receive();
            }
        });
    }

    fn report(&self, kind: OwnerError, report: String) -> (OwnerError, String) {
        let secondary = self
            .status
            .state
            .lock(|state| state.borrow().diagnostics.clone());
        if secondary.is_empty() {
            return crate::report::with_report(kind, report);
        }

        let mut tree = er_new!(kind.er_save("owner", report));
        for report in secondary {
            tree = tree.er_add(crate::report::DuringTeardown.er_save("report", report));
        }
        tree.into()
    }
}

#[cfg(feature = "std")]
impl Activity {
    fn phase(&self) -> AktorPhase {
        match self {
            Self::Call(operation) => operation.phase(),
            Self::Phase(phase) => phase.clone(),
        }
    }
}

impl Interval {
    fn arm(&mut self) {
        use crate::local::clock::AktorGroupClock;
        #[cfg(all(
            feature = "wasm_threads",
            target_family = "wasm",
            target_feature = "atomics"
        ))]
        use crate::local::clock::Browser as Clock;
        #[cfg(not(all(
            feature = "wasm_threads",
            target_family = "wasm",
            target_feature = "atomics"
        )))]
        use crate::local::clock::Embassy as Clock;

        self.ready = false;
        self.timer = Some(Clock::wait(Clock::deadline(self.every)));
    }
}

impl<S> Owner<S> {
    #[cfg(feature = "embassy_cross_core")]
    #[doc(hidden)]
    pub fn set_intervals(
        &mut self,
        intervals: Vec<crate::AktorInterval<S, crate::AktorKind::EmbassyCrossCore>>,
    ) where
        S: 'static,
    {
        for interval in intervals {
            self.add_interval(interval.every, interval.run);
        }
    }

    pub(crate) fn add_interval(
        &mut self,
        every: core::time::Duration,
        callback: crate::AktorClosure<dyn crate::setup::AktorIntervalLogic<S>>,
    ) {
        let index = self.hooks.intervals.len();
        self.hooks
            .intervals
            .push(Rc::new(RefCell::new(Some(callback))));
        self.intervals.push(Interval {
            every,
            callback: index,
            ready: false,
            timer: None,
        });
    }

    fn poll_intervals(&mut self, cx: &mut core::task::Context<'_>) {
        for interval in &mut self.intervals {
            if let Some(timer) = &mut interval.timer
                && timer.as_mut().poll(cx).is_ready()
            {
                interval.ready = true;
                interval.timer = None;
            }
        }
    }

    fn next_interval(&mut self) -> Option<usize> {
        let count = self.intervals.len();

        for offset in 0..count {
            let index = (self.interval_cursor + offset) % count;

            if self.intervals[index].ready {
                self.interval_cursor = (index + 1) % count;
                return Some(index);
            }
        }

        None
    }

    fn poll_work(
        &mut self,
        changed: &Listener,
        cx: &mut core::task::Context<'_>,
    ) -> Poll<Option<Work<S>>> {
        changed.register(cx);

        if self.inner.queue.lock(|queue| queue.borrow().open) {
            self.poll_intervals(cx);
        }

        for offset in 0..3 {
            let class = (self.work_cursor + offset) % 3;
            let work = if class == 2 {
                let open = self.inner.queue.lock(|queue| queue.borrow().open);

                if open {
                    self.next_interval().map(Work::Interval)
                } else {
                    None
                }
            } else {
                self.inner
                    .queue
                    .lock(|queue| {
                        let mut queue = queue.borrow_mut();

                        if class == 0 {
                            queue.ordinary.pop_front()
                        } else {
                            queue.services.pop_front()
                        }
                    })
                    .map(Work::Message)
            };

            #[cfg(all(test, feature = "std"))]
            if class == 0 && work.is_none() {
                let action = tests::EMPTY_QUEUE.with(|action| action.borrow_mut().take());

                if let Some(action) = action {
                    action();
                }
            }

            if let Some(work) = work {
                self.work_cursor = (class + 1) % 3;

                if class != 2 {
                    self.inner.changed.notify();
                }

                return Poll::Ready(Some(work));
            }
        }

        let (open, empty) = self.inner.queue.lock(|queue| {
            let queue = queue.borrow();

            (
                queue.open,
                queue.ordinary.is_empty() && queue.services.is_empty(),
            )
        });

        if !open && empty {
            Poll::Ready(None)
        } else {
            if !empty {
                cx.waker().wake_by_ref();
            }

            Poll::Pending
        }
    }

    #[cfg(feature = "embassy_cross_core")]
    pub fn manage(&mut self, name: alloc::string::String, kill: crate::embassy::AktorKillSwitch) {
        self.inner.status.state.lock(|state| {
            let mut state = state.borrow_mut();
            state.managed = true;
            state.actor_id = kill.actor_id();
        });
        self.supervisor = Some(Supervisor {
            name,
            kind: AktorExecution::EmbassyCrossCore,
            kill: kill.into(),
        });
    }

    fn remember(&self, id: Option<FailureId>) {
        if let (Some(supervisor), Some(id)) = (&self.supervisor, id)
            && let Some(delivery) = supervisor.kill.delivery(id)
        {
            self.inner
                .status
                .state
                .lock(|state| state.borrow_mut().returned.push(delivery));
        }
    }

    fn lost(&self, id: Option<FailureId>) {
        self.remember(id);
        self.inner.status.state.lock(|s| {
            let mut state = s.borrow_mut();
            if state.lost.is_none() {
                state.lost = state.returned.iter().find(|d| Some(d.0) == id).cloned();
            }
        });
    }

    fn fail(&self, phase: AktorPhase, diagnostic: &str) -> Option<FailureId> {
        if let Some(supervisor) = &self.supervisor {
            return Some(supervisor.fail(phase, diagnostic));
        }

        let id = FailureId::next();
        let delivery = crate::Delivery::new(id, diagnostic.into());
        self.inner
            .status
            .state
            .lock(|state| state.borrow_mut().returned.push(delivery));
        Some(id)
    }

    #[cfg(all(
        feature = "wasm_threads",
        target_family = "wasm",
        target_feature = "atomics"
    ))]
    pub(crate) fn manage_thread(&mut self, name: String, kill: crate::AktorKillSwitch) {
        self.inner.status.state.lock(|s| {
            let mut state = s.borrow_mut();
            state.managed = true;
            state.actor_id = kill.actor_id();
        });
        self.supervisor = Some(Supervisor {
            name,
            kind: AktorExecution::BrowserThread,
            kill: crate::local::StopSignal::Native {
                kill,
                standard: false,
            },
        });
    }

    #[cfg(feature = "std")]
    fn diagnostic(&self, phase: AktorPhase, error: String) {
        self.remember(self.fail(phase, &error));
        self.inner
            .status
            .state
            .lock(|state| state.borrow_mut().diagnostics.push(error));
    }

    #[cfg(feature = "std")]
    fn dispose_panic(&self, payload: Box<dyn std::any::Any + Send>) {
        dispose_with(payload, |nested| {
            self.diagnostic(AktorPhase::Drop, panic_tree(nested).er_report())
        });
    }

    #[cfg(feature = "std")]
    fn dispose_hooks(&mut self) -> Option<Box<dyn std::any::Any + Send>> {
        self.inner
            .status
            .state
            .lock(|s| s.borrow_mut().phase = Some(Activity::Phase(AktorPhase::Drop)));
        let hooks = core::mem::take(&mut self.hooks);
        let intervals = core::mem::take(&mut self.intervals);
        let report = |payload: &Box<dyn std::any::Any + Send>| {
            self.diagnostic(AktorPhase::Drop, panic_tree(payload).er_report());
        };

        let mut primary = panic::discard_hooks(hooks, report);

        if let Some(payload) = &primary {
            report(payload);
        }

        if let Some(payload) = panic::discard(intervals, report) {
            report(&payload);

            if primary.is_none() {
                primary = Some(payload);
            } else {
                self.dispose_panic(payload);
            }
        }

        primary
    }

    #[cfg(not(feature = "std"))]
    fn dispose_hooks(&mut self) {
        drop(core::mem::take(&mut self.hooks));
        drop(core::mem::take(&mut self.intervals));
    }

    pub async fn run_with<Start, StartFuture, End, EndFuture>(
        self,
        start: Start,
        end: End,
    ) -> Result<(), (OwnerError, String)>
    where
        Start: FnOnce() -> StartFuture,
        StartFuture: Future<Output = Result<S, (OwnerError, String)>>,
        End: FnOnce(S) -> EndFuture,
        EndFuture: Future<Output = Result<(), (OwnerError, String)>>,
    {
        let completion = Completion {
            status: self.inner.status.clone(),
        };
        let kind = Cell::new(OwnerError::Completion);
        let failed = |(error, report)| {
            kind.set(error);
            let (_, report) = crate::report::with_report(error, report);
            (RunErr { id: None }, report)
        };
        let running = self.run_prepared(
            async || match start().await {
                Ok(state) => Ok(state),
                Err(error) => Err(failed(error)),
            },
            async |state| {
                Some(match end(state).await {
                    Ok(()) => Ok(()),
                    Err(error) => Err(failed(error)),
                })
            },
        );
        #[cfg(feature = "std")]
        let result = panic::original(running).await;
        #[cfg(not(feature = "std"))]
        let result = running.await;

        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(completion.report(kind.get(), error.1)),
        }
    }

    pub(crate) async fn run_prepared<Start, StartFuture, End, EndFuture>(
        mut self,
        start: Start,
        end: End,
    ) -> Result<(), (RunErr, String)>
    where
        Start: FnOnce() -> StartFuture,
        StartFuture: Future<Output = Result<S, (RunErr, String)>>,
        End: FnOnce(S) -> EndFuture,
        EndFuture: Future<Output = Option<Result<(), (RunErr, String)>>>,
    {
        let inner = self.inner.clone();
        let cancel_changed = Event::listen(&inner.changed);
        #[cfg(feature = "std")]
        inner
            .status
            .state
            .lock(|s| s.borrow_mut().phase = Some(Activity::Phase(AktorPhase::Start)));
        let starting = async {
            let mut start = core::pin::pin!(async {
                inner
                    .status
                    .state
                    .lock(|s| s.borrow_mut().init = crate::AktorInitEvidence::Entered);
                if let Some(supervisor) = &self.supervisor {
                    supervisor
                        .kill
                        .init_evidence(crate::AktorInitEvidence::Entered);
                }
                start().await
            });

            poll_fn(|cx| {
                cancel_changed.register(cx);

                if inner.is_cancelled() {
                    return Poll::Ready(None);
                }

                start.as_mut().poll(cx).map(Some)
            })
            .await
        };

        #[cfg(feature = "std")]
        let started = panic::catch_pinned(Box::pin(starting), |dropped, payload, secondary| {
            let id = self.fail(
                if dropped {
                    AktorPhase::Drop
                } else {
                    AktorPhase::Start
                },
                &panic_tree(payload).er_report(),
            );
            if !secondary {
                self.lost(id);
            }
        })
        .await;
        #[cfg(feature = "std")]
        let started = match started {
            Ok(result) => result,
            Err(payload) => {
                let error = panic_tree(&payload).er_report();

                if let Err(secondary) = panic::catch(
                    async {
                        self.inner.finish(Err(error), None);
                    },
                    |_, secondary| {
                        self.diagnostic(AktorPhase::Drop, panic_tree(secondary).er_report());
                    },
                )
                .await
                {
                    self.dispose_panic(secondary);
                }

                if self.supervisor.is_some() {
                    std::panic::resume_unwind(Box::new(panic::Reported(payload, None)));
                }
                std::panic::resume_unwind(payload);
            }
        };
        #[cfg(not(feature = "std"))]
        let started = starting.await;

        let Some(started) = started else {
            inner.mark_cut();
            inner.finish(
                Err(er_new!(OwnerRunCause::StoppedDuringStart).er_report()),
                None,
            );
            return Ok(());
        };

        let mut state = match started {
            Ok(state) => state,
            Err(mut error) => {
                let report = error.1.clone();
                error.0.id = error.0.id.or_else(|| self.fail(AktorPhase::Start, &report));
                self.lost(error.0.id);
                self.inner.status.state.lock(|status| {
                    let mut status = status.borrow_mut();
                    status.cleanup_completed = true;

                    if status.managed && error.0.id.is_none() {
                        status.diagnostics.push(report.clone());
                    }
                });
                #[cfg(feature = "std")]
                if let Some(payload) = self.dispose_hooks() {
                    self.dispose_panic(payload);
                }
                #[cfg(not(feature = "std"))]
                self.dispose_hooks();
                self.inner.finish(Err(report), None);
                return Err(error);
            }
        };

        #[cfg(feature = "std")]
        let phase = |phase| {
            inner
                .status
                .state
                .lock(|s| s.borrow_mut().phase = Some(phase))
        };
        #[cfg(feature = "std")]
        phase(Activity::Phase(AktorPhase::Runner));
        for interval in &mut self.intervals {
            interval.arm();
        }

        self.inner
            .status
            .state
            .lock(|status| status.borrow_mut().ready = Some(Ok(())));
        self.inner.status.changed.notify();

        if let Some(supervisor) = &self.supervisor {
            supervisor
                .kill
                .init_evidence(crate::AktorInitEvidence::Ready);
        }
        let changed = Event::listen(&self.inner.changed);
        inner
            .status
            .state
            .lock(|s| s.borrow_mut().init = crate::AktorInitEvidence::Ready);
        #[cfg(feature = "std")]
        let supervisor = self.supervisor.clone();
        let mut running = Running {
            message: None,
            inner: inner.clone(),
            supervisor: self.supervisor.clone(),
        };
        let active = Cell::new(false);
        let serve = async {
            #[cfg(all(
                feature = "wasm_threads",
                target_family = "wasm",
                target_feature = "atomics"
            ))]
            let mut remaining = 32;

            loop {
                let work = poll_fn(|cx| self.poll_work(&changed, cx)).await;

                match work {
                    None => break,
                    Some(Work::Message(message)) => {
                        let message = running.message.insert(message);
                        #[cfg(feature = "std")]
                        phase(Activity::Call(message.operation));
                        active.set(true);
                        message
                            .job
                            .run(&mut state, &mut self.hooks, message.operation)
                            .await;
                        drop(running.message.take());
                    }
                    Some(Work::Interval(index)) => {
                        #[cfg(feature = "std")]
                        phase(Activity::Phase(AktorPhase::Interval));
                        active.set(true);
                        let callback = self.hooks.intervals[self.intervals[index].callback].clone();
                        let work = callback.borrow_mut().take();

                        if let Some(mut work) = work {
                            let operation = Operation::INTERVAL;

                            self.hooks.before(&mut state, operation);
                            work.0.run(&mut state).await;
                            self.hooks.after(&mut state, operation);

                            let previous = callback.replace(Some(work));

                            drop(previous);
                        }

                        self.intervals[index].arm();
                    }
                }
                active.set(false);
                #[cfg(feature = "std")]
                phase(Activity::Phase(AktorPhase::Runner));

                #[cfg(all(
                    feature = "wasm_threads",
                    target_family = "wasm",
                    target_feature = "atomics"
                ))]
                {
                    remaining -= 1;

                    // A browser thread's executor treats a self wake as "poll again right away",
                    // so a busy owner would never let the worker's own timers fire.
                    if remaining == 0 {
                        remaining = 32;
                        crate::timeout::browser_sleep(core::time::Duration::ZERO).await;
                        continue;
                    }
                }

                let mut yielded = false;

                poll_fn(|cx| {
                    if yielded {
                        Poll::Ready(())
                    } else {
                        yielded = true;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            }
        };
        // Dropping `serve` drops the call it was running, then `end` runs like after any stop.
        let serving = async {
            let mut serve = core::pin::pin!(serve);

            poll_fn(|cx| {
                cancel_changed.register(cx);

                if inner.is_cancelled() {
                    if active.get()
                        || inner.queue.lock(|queue| {
                            let queue = queue.borrow();
                            !queue.ordinary.is_empty() || !queue.services.is_empty()
                        })
                    {
                        inner.mark_cut();
                    }
                    return Poll::Ready(());
                }

                serve.as_mut().poll(cx)
            })
            .await
        };
        #[cfg(feature = "std")]
        let mut primary = panic::catch_pinned(Box::pin(serving), |dropped, payload, secondary| {
            let at = if dropped {
                AktorPhase::Drop
            } else {
                inner
                    .status
                    .state
                    .lock(|s| s.borrow().phase.as_ref().map(Activity::phase))
                    .unwrap_or(AktorPhase::Runner)
            };
            if let Some(supervisor) = &supervisor {
                let id = supervisor.fail(at, &panic_tree(payload).er_report());
                if !secondary && let Some(delivery) = supervisor.kill.delivery(id) {
                    inner.status.state.lock(|state| {
                        let mut state = state.borrow_mut();
                        state.lost.get_or_insert_with(|| delivery.clone());
                        state.returned.push(delivery);
                    });
                }
            } else if !secondary {
                let delivery =
                    crate::Delivery::new(FailureId::next(), panic_tree(payload).er_report());
                inner.status.state.lock(|state| {
                    let mut state = state.borrow_mut();
                    state.lost.get_or_insert_with(|| delivery.clone());
                    state.returned.push(delivery);
                });
            }
        })
        .await
        .err();
        #[cfg(not(feature = "std"))]
        serving.await;

        if inner.status.state.lock(|s| s.borrow().cut_call) {
            let report = er_new!(ShutdownCut).er_report();
            let id = self.fail(AktorPhase::Runner, &report);
            self.lost(id);
            if self.supervisor.is_none() {
                inner
                    .status
                    .state
                    .lock(|state| state.borrow_mut().diagnostics.push(report));
            }
        }

        drop(running);
        self.inner.close();
        #[cfg(feature = "std")]
        if let Err(payload) = panic::catch(
            async {
                self.inner.discard_queued();
            },
            |_, payload| {
                self.diagnostic(AktorPhase::Drop, panic_tree(payload).er_report());
            },
        )
        .await
        {
            if primary.is_none() {
                primary = Some(payload);
            } else {
                self.dispose_panic(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        self.inner.discard_queued();

        #[cfg(feature = "std")]
        phase(Activity::Phase(AktorPhase::End));
        let ending = async {
            // A managed owner's `end` gets one grace from when it starts, it says None past that.
            let Some(mut result) = end(state).await else {
                let mut error = er_new!(RunErr { id: None }, OwnerRunCause::EndTimedOut);
                error.top.id = self.fail(AktorPhase::End, &error.er_report());
                self.remember(error.top.id);
                self.inner.status.state.lock(|state| {
                    let mut state = state.borrow_mut();

                    state.ran_out = true;
                    #[cfg(feature = "std")]
                    if primary.is_some() {
                        state.diagnostics.push(error.er_report());
                    }
                });

                return Err(error.into());
            };

            let failure = if let Err(error) = &mut result {
                let report = error.1.clone();
                error.0.id = error.0.id.or_else(|| self.fail(AktorPhase::End, &report));
                self.remember(error.0.id);
                Some((error.0, report))
            } else {
                None
            };

            self.inner.status.state.lock(|state| {
                let mut state = state.borrow_mut();
                state.cleanup_completed = true;
                #[cfg(feature = "std")]
                if primary.is_some()
                    && let Some((_, report)) = &failure
                {
                    state.diagnostics.push(report.clone());
                }
                state.cleanup_failure = failure;
            });

            result
        };
        #[cfg(feature = "std")]
        let cleaned = panic::catch_pinned(Box::pin(ending), |dropped, payload, secondary| {
            let id = self.fail(
                if dropped {
                    AktorPhase::Drop
                } else {
                    AktorPhase::End
                },
                &panic_tree(payload).er_report(),
            );
            if !secondary {
                self.lost(id);
            }
        })
        .await;
        #[cfg(feature = "std")]
        let result = match cleaned {
            Ok(result) => result,
            Err(payload) => {
                let panic = panic_tree(&payload);
                if primary.is_some() {
                    let diagnostic = panic.er_report();
                    self.inner
                        .status
                        .state
                        .lock(|state| state.borrow_mut().diagnostics.push(diagnostic));
                }

                if primary.is_none() {
                    primary = Some(payload);
                } else {
                    self.dispose_panic(payload);
                }

                Err(
                    crate::report::unlocated(ErBranch::from(RunErr { id: None }).er_add(panic))
                        .into(),
                )
            }
        };
        #[cfg(not(feature = "std"))]
        let result = ending.await;
        #[cfg(feature = "std")]
        if let Some(payload) = self.dispose_hooks() {
            if primary.is_none() {
                primary = Some(payload);
            } else {
                self.dispose_panic(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        self.dispose_hooks();
        #[cfg(feature = "std")]
        let result = if let Some(payload) = &primary {
            Err(crate::report::unlocated(
                ErBranch::from(RunErr { id: None }).er_add(panic_tree(payload)),
            )
            .into())
        } else {
            result
        };
        #[cfg(feature = "std")]
        if let Err(payload) = panic::catch(
            async {
                self.inner.finish(
                    match &result {
                        Ok(()) => Ok(()),
                        Err(error) => Err(error.1.clone()),
                    },
                    None,
                );
            },
            |_, payload| {
                self.diagnostic(AktorPhase::Drop, panic_tree(payload).er_report());
            },
        )
        .await
        {
            if primary.is_none() {
                primary = Some(payload);
            } else {
                self.dispose_panic(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        self.inner.finish(
            match &result {
                Ok(()) => Ok(()),
                Err(error) => Err(error.1.clone()),
            },
            None,
        );
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            if self.supervisor.is_some() {
                std::panic::resume_unwind(Box::new(panic::Reported(payload, None)));
            }
            std::panic::resume_unwind(payload);
        }

        result
    }
}
struct Running<S> {
    message: Option<Message<S>>,
    inner: Arc<Inner<S>>,
    supervisor: Option<Supervisor>,
}

impl<S> Drop for Running<S> {
    fn drop(&mut self) {
        if self.message.is_some() {
            record_cancelled(
                &self.inner,
                self.supervisor.as_ref(),
                &er_new!(OwnerRunCause::OwnerCancelled).er_report(),
            );
        }
    }
}

fn record_cancelled<S>(inner: &Inner<S>, supervisor: Option<&Supervisor>, report: &str) {
    if inner
        .status
        .state
        .lock(|state| state.borrow().lost.is_some())
    {
        return;
    }
    let receipt = match supervisor {
        Some(owner) if owner.kill.is_stopping() => owner
            .kill
            .driver_failure()
            .and_then(|id| owner.kill.delivery(id)),
        Some(owner) => owner.kill.delivery(owner.fail(AktorPhase::Runner, report)),
        None => Some(crate::Delivery::new(FailureId::next(), report.into())),
    };
    if let Some(receipt) = receipt {
        inner.status.state.lock(|state| {
            let mut state = state.borrow_mut();
            state.lost = Some(receipt.clone());
            state.returned.push(receipt);
        });
    }
}

impl<S> Drop for Owner<S> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        let mut primary = self.dispose_hooks();
        #[cfg(not(feature = "std"))]
        self.dispose_hooks();

        if self
            .inner
            .status
            .state
            .lock(|state| state.borrow().completed.is_none())
        {
            let error = er_new!(OwnerRunCause::OwnerCancelled).er_report();

            record_cancelled(&self.inner, self.supervisor.as_ref(), &error);
            #[cfg(feature = "std")]
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.inner.finish(Err(error), None);
            })) {
                self.diagnostic(AktorPhase::Drop, panic_tree(&payload).er_report());

                if primary.is_none() {
                    primary = Some(payload);
                } else {
                    self.dispose_panic(payload);
                }
            }
            #[cfg(not(feature = "std"))]
            self.inner.finish(Err(error), None);
        }
        #[cfg(feature = "std")]
        for error in self
            .inner
            .status
            .state
            .lock(|s| core::mem::take(&mut s.borrow_mut().pending_drops))
        {
            self.fail(AktorPhase::Drop, &error);
        }
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            if std::thread::panicking() {
                self.dispose_panic(payload);
            } else if self.supervisor.is_some() {
                std::panic::resume_unwind(Box::new(panic::Reported(payload, None)));
            } else {
                std::panic::resume_unwind(payload);
            }
        }
    }
}

pin_project_lite::pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Run<F> {
        status: Arc<Status>,
        changed: Listener,
        #[pin]
        work: F,
    }
}
impl<F: Future> Future for Run<F> {
    type Output = Option<F::Output>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let this = self.project();
        this.changed.register(cx);

        let stopped = this.status.state.lock(|state| state.borrow().group_stopped);
        if stopped {
            return Poll::Ready(None);
        }

        this.work.poll(cx).map(Some)
    }
}

impl core::future::IntoFuture for Completion {
    type Output = Result<(), (OwnerError, String)>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}

#[derive(Clone, Copy, Er)]
pub(crate) struct RunErr {
    pub id: Option<FailureId>,
}

#[derive(Er)]
enum OwnerRunCause {
    StoppedDuringStart,
    EndTimedOut,
    OwnerCancelled,
    WorkDestroyed,
}

fn work_destroyed() -> String {
    er_new!(OwnerRunCause::WorkDestroyed).er_report()
}

struct CompletionGuard {
    status: Arc<Status>,
    fallback: Option<String>,
    armed: bool,
}
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.status
                .finish(Err(self.fallback.take().unwrap_or_else(work_destroyed)));
        }
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use core::task::{Context, Waker};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn cancelled() {
        use crate::latest::Session;

        let (handle, _owner) = channel::<()>(NonZeroUsize::MIN);

        for _ in 0..1000 {
            let (sender, results) = (&handle).session(
                Operation {
                    name: "cancelled",
                    caller: core::panic::Location::caller(),
                },
                async |_, input: u32| input,
            );
            sender.send(9);
            drop((sender, results));
        }

        assert!(
            handle
                .inner
                .queue
                .lock(|queue| queue.borrow().services.is_empty())
        );
    }

    std::thread_local! {
        pub static LAST_HANDLE: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
        pub static EMPTY_QUEUE: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = core::pin::pin!(future);

        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("available work did not finish"),
        }
    }

    #[test]
    fn panic_cleanup() {
        for setup in [false, true] {
            let mut group =
                crate::embassy::AktorGroup::with_grace(core::time::Duration::from_secs(1));
            let mut driver = group.listen(async |_| ()).unwrap();
            let (handle, mut owner) = channel::<u32>(NonZeroUsize::MIN);
            let completion = handle.completion();

            owner.manage("panicking actor".into(), group.killswitch());

            let request = Request::new(
                &handle,
                Operation {
                    name: "panic",
                    caller: core::panic::Location::caller(),
                },
                Box::new(request::WriteBody(async |state: &mut u32, ()| {
                    *state = 7;
                    panic!("operation failed");
                })),
                (),
            );

            ready(request.cast()).unwrap();

            let cleaned = Rc::new(RefCell::new(Vec::new()));
            let observed = cleaned.clone();
            let mut running = core::pin::pin!(owner.run_with(
                async move || {
                    if setup {
                        panic!("initialization failed");
                    }

                    Ok(0)
                },
                async move |state| {
                    let mut yielded = false;

                    poll_fn(|cx| {
                        if yielded {
                            Poll::Ready(())
                        } else {
                            yielded = true;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    })
                    .await;
                    observed.borrow_mut().push(state);
                    Ok(())
                }
            ));
            let mut cx = Context::from_waker(Waker::noop());
            let mut failure = None;

            for _ in 0..4 {
                if let Err(payload) =
                    catch_unwind(AssertUnwindSafe(|| running.as_mut().poll(&mut cx)))
                {
                    failure = Some(payload);
                    break;
                }
            }

            let reason = if setup {
                "initialization failed"
            } else {
                "operation failed"
            };

            assert_eq!(&**cleaned.borrow(), if setup { &[][..] } else { &[7][..] });
            assert_eq!(failure.unwrap().downcast_ref::<&str>(), Some(&reason));
            assert!(ready(completion.wait()).unwrap_err().1.contains(reason));

            let report = ready(&mut driver);

            assert_eq!(
                matches!(report.stop_cause, crate::StopCause::StartupFailure(_)),
                setup
            );
            assert!(report.failures[0].error.contains(reason));
        }
    }

    #[test]
    fn close_drain() {
        for explicit in [true, false] {
            let (handle, owner) = channel::<u32>(NonZeroUsize::MIN);
            let completion = handle.completion();
            let (send, empty) = std::sync::mpsc::channel();
            let producer = std::thread::spawn(move || {
                empty.recv().unwrap();
                let request = Request::new(
                    &handle,
                    Operation {
                        name: "increment",
                        caller: core::panic::Location::caller(),
                    },
                    Box::new(request::WriteBody(async |state: &mut u32, ()| {
                        *state += 1;
                    })),
                    (),
                );

                ready(request.cast()).unwrap();

                if explicit {
                    drop(handle.shutdown());
                }
            });

            EMPTY_QUEUE.with(|action| {
                *action.borrow_mut() = Some(Box::new(move || {
                    send.send(()).unwrap();
                    producer.join().unwrap();
                }));
            });

            let cleaned = Rc::new(RefCell::new(None));
            let observed = cleaned.clone();
            let mut running = core::pin::pin!(owner.run_with(
                async || Ok(0),
                async move |state| {
                    *observed.borrow_mut() = Some(state);
                    Ok(())
                }
            ));
            let mut cx = core::task::Context::from_waker(core::task::Waker::noop());

            for _ in 0..4 {
                if let Poll::Ready(result) = running.as_mut().poll(&mut cx) {
                    result.unwrap();
                    break;
                }
            }

            ready(completion.wait()).unwrap();
            assert_eq!(*cleaned.borrow(), Some(1), "accepted work was discarded");
        }
    }

    #[test]
    fn weak_upgrade() {
        let (handle, owner) = channel::<u32>(NonZeroUsize::MIN);
        let weak = handle.downgrade();
        let survivor = weak.upgrade().unwrap();

        drop(handle);

        let mut reply = ready(
            Request::new(
                &survivor,
                Operation {
                    name: "increment",
                    caller: core::panic::Location::caller(),
                },
                Box::new(request::WriteBody(async |state: &mut u32, ()| {
                    *state += 1;
                    *state
                })),
                (),
            )
            .send(),
        );

        drop(survivor);

        let mut running = core::pin::pin!(owner.run_with(
            async || Ok(0),
            async |state| {
                assert_eq!(state, 1);
                Ok(())
            }
        ));

        let mut cx = core::task::Context::from_waker(core::task::Waker::noop());

        assert!(running.as_mut().poll(&mut cx).is_pending());
        assert!(matches!(
            running.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(reply.as_mut().unwrap().try_take(), Some(Ok(1)));

        let (handle, owner) = channel::<u32>(NonZeroUsize::MIN);
        let weak = handle.downgrade();
        let acquired = Rc::new(RefCell::new(None));
        let store = acquired.clone();

        LAST_HANDLE.with(|action| {
            *action.borrow_mut() = Some(Box::new(move || {
                *store.borrow_mut() = weak.upgrade();
            }));
        });

        drop(handle);

        if let Some(handle) = acquired.borrow_mut().take() {
            assert!(handle.inner.queue.lock(|queue| queue.borrow().open));
            drop(handle);
        }

        assert!(!owner.inner.queue.lock(|queue| queue.borrow().open));
    }
}

#[cfg(test)]
mod arbitration_tests {
    use super::*;
    use core::task::{Context, Waker};

    struct Empty;
    impl Job<()> for Empty {
        fn run<'a>(
            &'a mut self,
            _: &'a mut (),
            _: &'a mut CallHooks<()>,
            _: Operation,
        ) -> LocalFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    fn message() -> Message<()> {
        Message {
            operation: Operation {
                name: "fairness",
                caller: core::panic::Location::caller(),
            },
            job: Box::new(Empty),
        }
    }

    #[test]
    fn work_classes() {
        for ordinary in [false, true] {
            let (_handle, mut owner) = channel::<()>(NonZeroUsize::MIN);

            owner.intervals.push(Interval {
                every: core::time::Duration::from_secs(1),
                callback: 0,
                ready: false,
                timer: None,
            });

            let changed = Event::listen(&owner.inner.changed);
            let mut cx = Context::from_waker(Waker::noop());
            let mut seen = [0; 3];

            for _ in 0..9 {
                if !owner.intervals[0].ready && owner.intervals[0].timer.is_none() {
                    let mut polls = 0;

                    owner.intervals[0].timer = Some(Box::pin(core::future::poll_fn(move |cx| {
                        polls += 1;

                        match polls {
                            1 => {
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                            2 => Poll::Ready(()),
                            _ => panic!("completed interval timer was polled again"),
                        }
                    })));
                }

                owner.inner.queue.lock(|queue| {
                    let mut queue = queue.borrow_mut();

                    if ordinary && queue.ordinary.is_empty() {
                        queue.ordinary.push_back(message());
                    }

                    if queue.services.is_empty() {
                        queue.services.push_back(message());
                    }
                });

                let work = owner.poll_work(&changed, &mut cx);

                match work {
                    Poll::Ready(Some(Work::Interval(index))) => {
                        seen[2] += 1;
                        owner.intervals[index].ready = false;
                        owner.intervals[index].timer = None;
                    }
                    Poll::Ready(Some(Work::Message(message))) => {
                        let service = owner
                            .inner
                            .queue
                            .lock(|queue| queue.borrow().services.is_empty());

                        seen[usize::from(service)] += 1;
                        drop(message);
                    }
                    _ => panic!("ready work was skipped"),
                }
            }

            assert!(
                seen[1] > 0 && seen[2] > 0 && (!ordinary || seen[0] > 0),
                "classes did not progress: {seen:?}"
            );
        }
    }
}
