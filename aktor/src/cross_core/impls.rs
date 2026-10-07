use super::*;
#[cfg(feature = "std")]
use crate::panic::{dispose_secondary, panic_message};
use crate::{
    ActorFailure, AktorCleanupError, AktorExecution, AktorSetupError, message::ActorError,
};
use alloc::string::ToString;
use core::{
    future::{Future, poll_fn},
    task::Poll,
};

/// The target's critical-section implementation must synchronize every calling core.
pub fn channel<S>(capacity: usize) -> Result<(Handle<S>, Owner<S>), ActorError> {
    if capacity == 0 {
        return Err(ActorError::InvalidCapacity);
    }

    let inner = Arc::new(Inner {
        queue: Mutex::new(RefCell::new(Queue {
            open: true,
            handles: 1,
            ordinary: VecDeque::new(),
            services: VecDeque::new(),
        })),
        capacity,
        changed: Arc::new(Event::default()),
        status: Arc::new(Status {
            state: Mutex::new(RefCell::new(State::default())),
            changed: Arc::new(Event::default()),
        }),
    });

    Ok((
        Handle {
            inner: inner.clone(),
            role: PhantomData,
        },
        Owner {
            inner,
            hooks: AktorHooks::default(),
            supervisor: None,
            local: PhantomData,
            intervals: Vec::new(),
            interval_cursor: 0,
            work_cursor: 0,
        },
    ))
}

impl<S> Inner<S> {
    pub fn close(&self) {
        self.queue.lock(|queue| {
            self.close_queue(&mut queue.borrow_mut());
        });
        self.changed.notify();
        self.status.changed.notify();
    }

    fn close_queue(&self, queue: &mut Queue<S>) {
        queue.open = false;
        self.status
            .state
            .lock(|state| state.borrow_mut().closing = true);
    }

    pub fn finish(&self, result: Result<(), AktorError>) {
        self.close();

        let mut completion = CompletionGuard {
            status: self.status.clone(),
            fallback: result
                .as_ref()
                .err()
                .cloned()
                .unwrap_or_else(|| AktorError::new("actor work destruction failed")),
            armed: true,
        };

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
        let primary =
            crate::local::panic::discard(ordinary.into_iter().chain(services), |payload| {
                self.status.state.lock(|state| {
                    state
                        .borrow_mut()
                        .diagnostics
                        .push(AktorError::new(panic_message(payload)))
                });
            });
        #[cfg(not(feature = "std"))]
        for message in ordinary.into_iter().chain(services) {
            drop(message);
        }
        #[cfg(feature = "std")]
        let result = if result.is_ok() && primary.is_some() {
            Err(completion.fallback.clone())
        } else {
            result
        };

        self.status.finish(result);
        completion.armed = false;
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }
    }

    pub fn commit(&self, message: Message<S>, service: bool) -> Result<(), Message<S>> {
        self.queue.lock(|queue| {
            let mut queue = queue.borrow_mut();

            if service {
                if self.status.state.lock(|state| state.borrow().finishing) {
                    return Err(message);
                }

                queue.services.push_back(message);
            } else {
                if !queue.open || queue.ordinary.len() == self.capacity {
                    return Err(message);
                }

                queue.ordinary.push_back(message);
            }

            Ok(())
        })
    }
}

impl Status {
    pub fn finish(&self, result: Result<(), AktorError>) {
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

    pub fn managed(&self) -> bool {
        self.state
            .lock(|state| state.borrow().group_stopping.is_some())
    }

    pub fn stopping(&self) -> bool {
        self.state.lock(|state| {
            state
                .borrow()
                .group_stopping
                .as_ref()
                .is_some_and(|stopping| stopping.load(Ordering::Acquire))
        })
    }
}

impl<S, Role> Handle<S, Role> {
    pub fn new_handle(&self) -> Self {
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

    pub fn shutdown(&self) -> Completion {
        self.inner.close();
        self.completion()
    }

    pub fn completion(&self) -> Completion {
        Completion {
            status: self.inner.status.clone(),
        }
    }

    pub async fn ready(&self) -> Result<(), AktorError> {
        let changed = Event::listen(&self.inner.status.changed);

        poll_fn(|cx| {
            changed.register(cx);
            self.inner
                .status
                .state
                .lock(|state| state.borrow().ready.clone())
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await
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
            self.inner.changed.notify();
            self.inner.status.changed.notify();
        }
    }
}

impl<S, Role> WeakHandle<S, Role> {
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
    #[doc(hidden)]
    pub fn diagnostics(&self) -> Vec<AktorError> {
        self.status
            .state
            .lock(|state| state.borrow().diagnostics.clone())
    }

    pub async fn wait(&self) -> Result<(), AktorError> {
        let changed = Event::listen(&self.status.changed);

        poll_fn(|cx| {
            changed.register(cx);
            self.status
                .state
                .lock(|state| state.borrow().completed.clone())
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await
    }
}

impl Interval {
    fn arm(&mut self) {
        let deadline = crate::timeout::embassy_deadline(embassy_time::Instant::now(), self.every);
        self.ready = false;
        self.timer = Some(Box::pin(embassy_time::Timer::at(deadline)));
    }
}

impl<S> Owner<S> {
    #[doc(hidden)]
    pub fn set_intervals(
        &mut self,
        intervals: Vec<(
            core::time::Duration,
            crate::AktorClosure<dyn crate::setup::AktorIntervalLogic<S>>,
        )>,
    ) -> Result<(), AktorError> {
        if intervals.iter().any(|(every, _)| every.is_zero()) {
            return Err(AktorError::new("interval duration must be positive"));
        }

        for (every, callback) in intervals {
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

        Ok(())
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

    pub fn manage(&mut self, name: alloc::string::String, kill: crate::embassy::KillSwitch) {
        self.inner.status.state.lock(|state| {
            let mut state = state.borrow_mut();
            state.group_stopping = Some(kill.shared_stopping());
        });
        self.supervisor = Some((name, kill));
    }

    fn fail(&self, phase: &str, error: &AktorError) {
        if let Some((name, kill)) = &self.supervisor {
            kill.fail(ActorFailure {
                actor: name.clone(),
                kind: Some(AktorExecution::EmbassyCrossCore),
                phase: phase.into(),
                message: error.to_string(),
            });
        }
    }

    fn fail_startup(&self, error: &AktorError) {
        if let Some((name, kill)) = &self.supervisor {
            kill.fail_startup(ActorFailure {
                actor: name.clone(),
                kind: Some(AktorExecution::EmbassyCrossCore),
                phase: "setup".into(),
                message: error.to_string(),
            });
        }
    }

    fn diagnostic(&self, phase: &str, error: AktorError) {
        self.fail(phase, &error);
        self.inner
            .status
            .state
            .lock(|state| state.borrow_mut().diagnostics.push(error.clone()));

        if let Some((name, kill)) = &self.supervisor {
            kill.record_diagnostic(AktorError::new(alloc::format!("{name}: {error}")));
        }
    }

    #[cfg(feature = "std")]
    fn dispose_hooks(&mut self) -> Option<Box<dyn std::any::Any + Send>> {
        let hooks = core::mem::take(&mut self.hooks);
        let intervals = core::mem::take(&mut self.intervals);
        let report = |payload: &Box<dyn std::any::Any + Send>| {
            self.diagnostic("hook drop", AktorError::new(panic_message(payload)));
        };

        let mut primary = crate::local::panic::discard_hooks(hooks, report);

        if let Some(payload) = &primary {
            report(payload);
        }

        if let Some(payload) = crate::local::panic::discard(intervals, report) {
            report(&payload);

            if primary.is_none() {
                primary = Some(payload);
            } else {
                dispose_secondary(payload);
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
        mut self,
        start: Start,
        end: End,
    ) -> Result<(), AktorError>
    where
        Start: FnOnce() -> StartFuture,
        StartFuture: Future<Output = Result<S, AktorSetupError>>,
        End: FnOnce(S) -> EndFuture,
        EndFuture: Future<Output = Result<(), AktorCleanupError>>,
    {
        #[cfg(feature = "std")]
        let started = crate::local::panic::catch(async { start().await }, |_, payload| {
            self.fail_startup(&AktorError::new(panic_message(payload)));
        })
        .await;
        #[cfg(feature = "std")]
        let started = match started {
            Ok(result) => result,
            Err(payload) => {
                let error = AktorError::new(panic_message(&payload));

                if let Err(secondary) = crate::local::panic::catch(
                    async {
                        self.inner.finish(Err(error));
                    },
                    |_, secondary| {
                        self.diagnostic("queue drop", AktorError::new(panic_message(secondary)));
                    },
                )
                .await
                {
                    dispose_secondary(secondary);
                }

                std::panic::resume_unwind(payload);
            }
        };
        #[cfg(not(feature = "std"))]
        let started = start().await;

        let mut state = match started {
            Ok(state) => state,
            Err(error) => {
                self.fail_startup(&error);
                #[cfg(feature = "std")]
                if let Some(payload) = self.dispose_hooks() {
                    dispose_secondary(payload);
                }
                #[cfg(not(feature = "std"))]
                self.dispose_hooks();
                self.inner.finish(Err(error.clone()));
                return Err(error);
            }
        };

        for interval in &mut self.intervals {
            interval.arm();
        }

        self.inner
            .status
            .state
            .lock(|status| status.borrow_mut().ready = Some(Ok(())));
        self.inner.status.changed.notify();

        let changed = Event::listen(&self.inner.changed);
        let serving = async {
            loop {
                let work = poll_fn(|cx| self.poll_work(&changed, cx)).await;

                match work {
                    None => break,
                    Some(Work::Message(mut message)) => {
                        message
                            .job
                            .run(&mut state, &mut self.hooks, message.operation)
                            .await;
                    }
                    Some(Work::Interval(index)) => {
                        let callback = self.hooks.intervals[self.intervals[index].callback].clone();
                        let work = callback.borrow_mut().take();

                        if let Some(mut work) = work {
                            let operation = Operation {
                                name: "interval",
                                caller: core::panic::Location::caller(),
                            };

                            self.hooks.before(&mut state, operation);
                            work.0.run(&mut state).await;
                            self.hooks.after(&mut state, operation);

                            let previous = callback.replace(Some(work));

                            drop(previous);
                        }

                        self.intervals[index].arm();
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
        #[cfg(feature = "std")]
        let mut primary = crate::local::panic::catch(serving, |_, _| {}).await.err();
        #[cfg(feature = "std")]
        if let Some(payload) = &primary {
            self.fail("operation", &AktorError::new(panic_message(payload)));
        }
        #[cfg(not(feature = "std"))]
        serving.await;

        self.inner.close();
        #[cfg(feature = "std")]
        let cleaned = crate::local::panic::catch(async { end(state).await }, |_, payload| {
            self.fail("cleanup", &AktorError::new(panic_message(payload)));
        })
        .await;
        #[cfg(feature = "std")]
        let result = match cleaned {
            Ok(result) => result,
            Err(payload) => {
                let error = AktorError::new(panic_message(&payload));

                if primary.is_none() {
                    primary = Some(payload);
                } else {
                    dispose_secondary(payload);
                }

                Err(error)
            }
        };
        #[cfg(not(feature = "std"))]
        let result = end(state).await;

        if let Err(error) = &result {
            self.diagnostic("cleanup", error.clone());
        }
        #[cfg(feature = "std")]
        if let Some(payload) = self.dispose_hooks() {
            if primary.is_none() {
                primary = Some(payload);
            } else {
                dispose_secondary(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        self.dispose_hooks();
        #[cfg(feature = "std")]
        let result = if let Some(payload) = &primary {
            Err(AktorError::new(panic_message(payload)))
        } else {
            result
        };
        #[cfg(feature = "std")]
        if let Err(payload) = crate::local::panic::catch(
            async {
                self.inner.finish(result.clone());
            },
            |_, _| {},
        )
        .await
        {
            self.diagnostic("queue drop", AktorError::new(panic_message(&payload)));

            if primary.is_none() {
                primary = Some(payload);
            } else {
                dispose_secondary(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        self.inner.finish(result.clone());
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            std::panic::resume_unwind(payload);
        }

        result
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
            let error = AktorError::new("actor owner cancelled before cleanup completed");

            if !self
                .supervisor
                .as_ref()
                .is_some_and(|(_, kill)| kill.is_stopping())
            {
                self.fail("owner", &error);
            }
            #[cfg(feature = "std")]
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.inner.finish(Err(error));
            })) {
                self.diagnostic("queue drop", AktorError::new(panic_message(&payload)));

                if primary.is_none() {
                    primary = Some(payload);
                } else {
                    dispose_secondary(payload);
                }
            }
            #[cfg(not(feature = "std"))]
            self.inner.finish(Err(error));
        }
        #[cfg(feature = "std")]
        if let Some(payload) = primary {
            if std::thread::panicking() {
                dispose_secondary(payload);
            } else {
                std::panic::resume_unwind(payload);
            }
        }
    }
}

impl core::future::IntoFuture for Completion {
    type Output = Result<(), AktorError>;
    type IntoFuture = core::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.wait().await })
    }
}

struct CompletionGuard {
    status: Arc<Status>,
    fallback: AktorError,
    armed: bool,
}
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.status.finish(Err(self.fallback.clone()));
        }
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use core::task::{Context, Waker};
    use std::panic::{AssertUnwindSafe, catch_unwind};

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
            let mut driver = group.listen().unwrap();
            let (handle, mut owner) = channel::<u32>(1).unwrap();
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

            ready(request.cast());

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
            assert_eq!(ready(completion.wait()).unwrap_err().diagnostics, reason);

            let report = ready(&mut driver);

            assert_eq!(report.startup, setup);
            assert_eq!(report.failure.unwrap().message, reason);
        }
    }

    #[test]
    fn close_drain() {
        for explicit in [true, false] {
            let (handle, owner) = channel::<u32>(1).unwrap();
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

                ready(request.cast());

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
        let (handle, owner) = channel::<u32>(1).unwrap();
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
        assert_eq!(reply.try_take(), Some(1));

        let (handle, owner) = channel::<u32>(1).unwrap();
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
            _: &'a mut AktorHooks<()>,
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
            let (_handle, mut owner) = channel::<()>(1).unwrap();

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
