mod stopping;
pub use stopping::{AktorStopGuard, AktorStopping};

use crate::queue::JobPermit;
use crate::{AktorFailureInput, AktorJob, AktorJobOutput, AktorPhase, Delivery, queue::Admission};
use alloc::{boxed::Box, vec::Vec};
use core::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context as TaskContext, Poll},
};
use er::*;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered, task::AtomicWaker};
use std::{
    any::Any,
    cell::{Cell, RefCell},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    thread,
};

#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
pub type LocalWork = Pin<Box<dyn Future<Output = ()>>>;
#[cfg(all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")))]
pub type SendWork = Pin<Box<dyn Future<Output = ()> + Send>>;
/// A browser task actor never leaves its thread, its work doesn't need to either.
#[cfg(all(feature = "bevy", target_family = "wasm"))]
pub type SendWork = Pin<Box<dyn Future<Output = ()>>>;

std::thread_local! {
    /// The actor being polled on this thread right now, for `aktor_spawn` and friends.
    static ACTIVE: RefCell<Option<Active>> = const { RefCell::new(None) };
    /// A thread actor stays in `ACTIVE` for its whole thread, this says it's being polled. A Tokio
    /// task on the same thread runs outside of that and doesn't belong to the actor.
    static POLLING: Cell<bool> = const { Cell::new(false) };
}

/// The running actor as the free functions see it. Thread kinds take `!Send` work too, task kinds
/// move between threads so theirs has to be `Send`.
#[derive(Clone)]
pub(crate) enum Active {
    #[cfg(all(
        any(feature = "tokio", feature = "std_thread"),
        not(target_family = "wasm")
    ))]
    Local(Arc<Context<LocalWork>>),
    #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
    Send(Arc<Context<SendWork>>),
}

/// Spawned work as one kind of actor holds it.
pub(crate) trait Work: Future<Output = ()> + Unpin + Sized + 'static {
    /// The actor owns its thread, so it goes into `ACTIVE` once instead of on every poll.
    const STAYS: bool;

    /// The actor's own work, like its intervals. A browser task actor never leaves its thread.
    #[cfg(not(target_family = "wasm"))]
    fn boxed(work: impl Future<Output = ()> + Send + 'static) -> Self;
    #[cfg(target_family = "wasm")]
    fn boxed(work: impl Future<Output = ()> + 'static) -> Self;

    fn active(context: Arc<Context<Self>>) -> Active;
    fn context(active: Active) -> Option<Arc<Context<Self>>>;
}
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
impl Work for LocalWork {
    const STAYS: bool = true;

    fn boxed(work: impl Future<Output = ()> + Send + 'static) -> Self {
        Box::pin(work)
    }

    fn active(context: Arc<Context<Self>>) -> Active {
        Active::Local(context)
    }

    fn context(active: Active) -> Option<Arc<Context<Self>>> {
        match active {
            Active::Local(context) => Some(context),
            #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
            Active::Send(_) => None,
        }
    }
}
#[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
impl Work for SendWork {
    const STAYS: bool = false;

    #[cfg(not(target_family = "wasm"))]
    fn boxed(work: impl Future<Output = ()> + Send + 'static) -> Self {
        Box::pin(work)
    }

    #[cfg(target_family = "wasm")]
    fn boxed(work: impl Future<Output = ()> + 'static) -> Self {
        Box::pin(work)
    }

    fn active(context: Arc<Context<Self>>) -> Active {
        Active::Send(context)
    }

    fn context(active: Active) -> Option<Arc<Context<Self>>> {
        match active {
            Active::Send(context) => Some(context),
            #[cfg(all(
                any(feature = "tokio", feature = "std_thread"),
                not(target_family = "wasm")
            ))]
            Active::Local(_) => None,
        }
    }
}

/// What the running actor shares with work spawned inside it.
pub(crate) struct Context<W> {
    owner: Arc<JobOwner>,
    admission: Arc<Admission>,
    pending: AtomicBool,
    inbox: parking_lot::Mutex<Vec<Tracked<W>>>,
    retire: Arc<dyn Fn() + Send + Sync>,
}

#[derive(Clone, Copy, PartialEq, Eq, Er)]
pub enum AktorSpawnError {
    NoOwner,
    Closing,
    Full,
}

#[derive(Clone, Copy, PartialEq, Eq, Er)]
pub enum AktorJobError {
    Failed(crate::FailureId),
    Cut(crate::FailureId),
    Dropped,
}

pub(crate) struct JobOwner {
    pub starting: AtomicBool,
    pub admission: std::sync::Weak<Admission>,
    pub name: String,
    pub kind: crate::AktorExecution,
    pub kill: crate::AktorKillSwitch,
}
impl JobOwner {
    fn failed(&self, phase: AktorPhase, report: String) -> Delivery {
        self.kill.record_failure(
            AktorFailureInput::actor(self.name.clone(), self.kind, phase, report),
            self.starting.load(Ordering::Acquire),
            |_| {},
        )
    }

    fn unwinding(&self, payload: Box<dyn Any + Send>) {
        crate::panic::each(payload, |payload, _| {
            let record = |p: &Box<dyn Any + Send>| {
                self.failed(AktorPhase::Drop, crate::panic::panic_tree(p).er_report());
            };
            record(&payload);
            crate::panic::dispose_with(payload, record);
        });
    }
}

/// Dropping the receipt leaves the job running on its actor.
pub struct AktorOwnerTask<T> {
    shared: Arc<Shared>,
    output: Arc<parking_lot::Mutex<Option<T>>>,
}

pub(crate) struct Shared {
    pub(crate) policy: AktorJob,
    caller: &'static core::panic::Location<'static>,
    result: parking_lot::Mutex<JobResult>,
    aborted: AtomicBool,
    finished: AtomicBool,
    task: AtomicWaker,
    joiner: AtomicWaker,
}

enum JobResult {
    Running,
    Returned,
    Cutting,
    Error(AktorJobError, String, Delivery),
}

struct Tracked<W> {
    owner: Arc<JobOwner>,
    work: Option<W>,
    shared: Arc<Shared>,
    permit: Option<JobPermit>,
}

type ExternalWork = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Default)]
pub(crate) struct External {
    pub owner: std::sync::OnceLock<Arc<JobOwner>>,
    inbox: parking_lot::Mutex<Vec<Tracked<ExternalWork>>>,
    wake: AtomicWaker,
    pending: AtomicBool,
}

enum Running<W> {
    Inside(Tracked<W>),
    Outside(Tracked<ExternalWork>),
}
impl<W> Running<W> {
    fn shared(&self) -> &Shared {
        match self {
            Self::Inside(job) => &job.shared,
            Self::Outside(job) => &job.shared,
        }
    }
}
impl<W: Work> Future for Running<W> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        match self.get_mut() {
            Self::Inside(job) => Pin::new(job).poll(cx),
            Self::Outside(job) => Pin::new(job).poll(cx),
        }
    }
}

/// The work spawned inside one actor, polled by the actor itself. Spawning works while this
/// lives, and the work goes with it.
pub struct Spawned<W: Work> {
    running: FuturesUnordered<Running<W>>,
    external: Arc<External>,
    context: Arc<Context<W>>,
}

pin_project_lite::pin_project! {
    /// Makes the actor visible to `aktor_spawn` while its future is polled.
    pub(crate) struct Scoped<W, F> {
        context: Option<Arc<Context<W>>>,
        // A thread actor took its place in `ACTIVE` for good, it clears it when it's done.
        stays: bool,
        #[pin]
        future: F,
    }
    impl<W, F> PinnedDrop for Scoped<W, F> {
        fn drop(this: Pin<&mut Self>) {
            if this.stays {
                let ours = ACTIVE.with(|active| active.replace(None));

                drop(ours);
            }
        }
    }
}

/// Run work beside this actor's calls. Use `FinishBeforeEnd` when cleanup needs the job to finish.
#[track_caller]
pub fn aktor_spawn<O, E>(
    work: impl Future<Output = O> + Send + 'static,
    policy: AktorJob,
) -> Result<AktorOwnerTask<O::Value>, (AktorSpawnError, String)>
where
    O: AktorJobOutput<E> + 'static,
    O::Value: Send + 'static,
{
    let active = ACTIVE.with(|active| active.borrow().clone());
    match active {
        #[cfg(all(
            any(feature = "tokio", feature = "std_thread"),
            not(target_family = "wasm")
        ))]
        Some(Active::Local(context)) if POLLING.get() => context.spawn(work, policy),
        #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
        Some(Active::Send(context)) => context.spawn(work, policy),
        _ => {
            drop(work);
            let _ = policy;
            er_bail!(AktorSpawnError::NoOwner)
        }
    }
}

/// Run work that can hold thread local state on a dedicated actor thread.
#[cfg(any(
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
#[track_caller]
pub fn aktor_spawn_local<O, E>(
    work: impl Future<Output = O> + 'static,
    policy: AktorJob,
) -> Result<AktorOwnerTask<O::Value>, (AktorSpawnError, String)>
where
    O: AktorJobOutput<E> + 'static,
    O::Value: 'static,
{
    #[cfg(not(target_family = "wasm"))]
    if let Some(Active::Local(context)) = ACTIVE.with(|active| active.borrow().clone())
        && POLLING.get()
    {
        return context.spawn(work, policy);
    }

    let _ = (work, policy);
    er_bail!(AktorSpawnError::NoOwner)
}

/// The running actor's shutdown signal.
#[track_caller]
pub fn aktor_stopping() -> AktorStopping {
    ACTIVE
        .with(|active| active.borrow().as_ref().and_then(Active::stopping))
        .expect("aktor_stopping needs a running actor")
}

/// Retire the running actor from `start`, a call or spawned work, on `TokioThread`, `StdThread`,
/// `TokioTask` or `BevyTask`. Panics outside a supported actor. Starts group shutdown, discards
/// queued calls and runs `end` after the current turn.
#[track_caller]
pub fn aktor_retire() {
    let retire = ACTIVE.with(|active| active.borrow().as_ref().and_then(Active::retire));

    match retire {
        Some(retire) => retire(),
        None => panic!(
            "aktor_retire only works inside a running actor's start, call, end or spawned work"
        ),
    }
}

impl Active {
    fn stopping(&self) -> Option<AktorStopping> {
        match self {
            #[cfg(all(
                any(feature = "tokio", feature = "std_thread"),
                not(target_family = "wasm")
            ))]
            Self::Local(context) => POLLING.get().then(|| context.stopping()),
            #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
            Self::Send(context) => Some(context.stopping()),
            #[cfg(not(any(
                all(
                    any(feature = "tokio", feature = "std_thread"),
                    not(target_family = "wasm")
                ),
                feature = "bevy"
            )))]
            _ => match *self {},
        }
    }

    fn retire(&self) -> Option<Arc<dyn Fn() + Send + Sync>> {
        match self {
            #[cfg(all(
                any(feature = "tokio", feature = "std_thread"),
                not(target_family = "wasm")
            ))]
            Self::Local(context) => POLLING.get().then(|| context.retire.clone()),
            #[cfg(any(all(feature = "tokio", not(target_family = "wasm")), feature = "bevy"))]
            Self::Send(context) => Some(context.retire.clone()),
            #[cfg(not(any(
                all(
                    any(feature = "tokio", feature = "std_thread"),
                    not(target_family = "wasm")
                ),
                feature = "bevy"
            )))]
            _ => match *self {},
        }
    }
}

impl<W> Context<W> {
    fn stopping(&self) -> AktorStopping {
        AktorStopping::new(self.admission.clone(), self.owner.clone())
    }
}

#[cfg(any(not(target_family = "wasm"), feature = "bevy"))]
macro_rules! inside_spawn {
    ($work:ty $(, $send:ident)?) => {
        impl Context<$work> {
            #[track_caller]
            fn spawn<O, E>(
                &self,
                work: impl Future<Output = O> $(+ $send)? + 'static,
                policy: AktorJob,
            ) -> Result<AktorOwnerTask<O::Value>, (AktorSpawnError, String)>
            where
                O: AktorJobOutput<E> + 'static,
                O::Value: $($send +)? 'static,
            {
                let (receipt, job) = AktorOwnerTask::new(work, policy, self.owner.clone(), O::into_job);
                self.admission.admit_job(&receipt.shared, |permit| {
                    self.inbox.lock().push(Tracked {
                        owner: self.owner.clone(),
                        shared: receipt.shared.clone(),
                        work: Some(Box::pin(job)),
                        permit: Some(permit),
                    });
                    self.pending.store(true, Ordering::Release);
                })?;
                Ok(receipt)
            }
        }
    };
}
#[cfg(all(
    any(feature = "tokio", feature = "std_thread"),
    not(target_family = "wasm")
))]
inside_spawn!(LocalWork);
#[cfg(all(any(feature = "tokio", feature = "bevy"), not(target_family = "wasm")))]
inside_spawn!(SendWork, Send);
#[cfg(all(feature = "bevy", target_family = "wasm"))]
inside_spawn!(SendWork);

impl External {
    #[track_caller]
    pub(crate) fn spawn<O, E>(
        &self,
        admission: &Arc<Admission>,
        owner: Arc<JobOwner>,
        work: impl Future<Output = O> + Send + 'static,
        policy: AktorJob,
    ) -> Result<AktorOwnerTask<O::Value>, (AktorSpawnError, String)>
    where
        O: AktorJobOutput<E> + 'static,
        O::Value: Send + 'static,
    {
        let (receipt, job) = AktorOwnerTask::new(work, policy, owner.clone(), O::into_job);
        admission.admit_job(&receipt.shared, |permit| {
            self.inbox.lock().push(Tracked {
                owner,
                shared: receipt.shared.clone(),
                work: Some(Box::pin(job)),
                permit: Some(permit),
            });
            self.pending.store(true, Ordering::Release);
        })?;
        self.wake.wake();
        Ok(receipt)
    }
}

impl Drop for External {
    fn drop(&mut self) {
        // The executor can lose the owner before it ever polls its inbox.
        for job in self.inbox.get_mut().drain(..) {
            let owner = job.owner.clone();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(job))) {
                owner.unwinding(payload);
            }
        }
    }
}

impl Shared {
    #[track_caller]
    fn new(policy: AktorJob) -> Self {
        Self {
            policy,
            caller: core::panic::Location::caller(),
            result: parking_lot::Mutex::new(JobResult::Running),
            aborted: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            task: AtomicWaker::new(),
            joiner: AtomicWaker::new(),
        }
    }

    fn fail(&self, owner: &JobOwner, error: fn(crate::FailureId) -> AktorJobError, report: String) {
        let delivery = owner.failed(AktorPhase::Background, report.clone());
        let error = error(delivery.0);
        if matches!(error, AktorJobError::Failed(_))
            && let Some(admission) = owner.admission.upgrade()
        {
            let _ = admission.terminal.set(delivery.clone());
        }
        *self.result.lock() = JobResult::Error(error, report, delivery);
    }

    pub(crate) fn stop(&self) {
        self.aborted.store(true, Ordering::Release);
        self.task.wake();
    }

    pub(crate) fn cut(&self, owner: &JobOwner) {
        {
            let mut result = self.result.lock();
            if self.aborted.load(Ordering::Acquire) || !matches!(*result, JobResult::Running) {
                return;
            }
            *result = JobResult::Cutting;
        }

        self.fail(
            owner,
            AktorJobError::Cut,
            er_new!(JobCut {
                spawned_at: self.caller
            })
            .er_report(),
        );
        self.aborted.store(true, Ordering::Release);
        for wake in [&self.task, &self.joiner] {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| wake.wake())) {
                owner.unwinding(payload);
            }
        }
    }
}

#[derive(Er)]
struct JobFailed;
#[derive(Er)]
#[er(format = "background job spawned at {spawned_at} cut at shutdown grace")]
struct JobCut {
    spawned_at: &'static core::panic::Location<'static>,
}
impl<T> AktorOwnerTask<T> {
    #[track_caller]
    fn new<O: 'static, F>(
        work: F,
        policy: AktorJob,
        owner: Arc<JobOwner>,
        convert: fn(O) -> Result<T, String>,
    ) -> (Self, impl Future<Output = ()> + 'static)
    where
        F: Future<Output = O> + 'static,
        T: 'static,
    {
        let shared = Arc::new(Shared::new(policy));
        let output = Arc::new(parking_lot::Mutex::new(None));
        let slot = output.clone();
        let job = shared.clone();
        let work = work.map(move |value| {
            let result = convert(value);
            {
                let mut state = job.result.lock();
                if !matches!(*state, JobResult::Running) {
                    drop(state);
                    if let Err(error) = result {
                        owner.failed(
                            AktorPhase::Background,
                            er_new!(JobFailed.er_save("job", error)).er_report(),
                        );
                    }
                    return;
                }
                *state = JobResult::Returned;
            }

            match result {
                Ok(value) => *slot.lock() = Some(value),
                Err(error) => job.fail(
                    &owner,
                    AktorJobError::Failed,
                    er_new!(JobFailed.er_save("job", error)).er_report(),
                ),
            }
        });

        (Self { shared, output }, work)
    }

    /// Stop the work at its next turn. It's dropped on the owner, never in the middle of a poll.
    /// Works from any thread.
    pub fn abort(&self) {
        self.shared.stop();
    }

    /// The work is gone: finished, aborted or dropped with the actor, its drop already ran.
    pub fn is_finished(&self) -> bool {
        self.shared.finished.load(Ordering::Acquire)
    }
}
impl<T> Future for AktorOwnerTask<T> {
    type Output = Result<T, (AktorJobError, String)>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        self.shared.joiner.register(cx.waker());
        let result = self.shared.result.lock();
        if let JobResult::Error(error, text, delivery) = &*result
            && (self.is_finished() || matches!(error, AktorJobError::Cut(_)))
        {
            delivery.receive();
            return Poll::Ready(Err((*error, text.clone())));
        }
        if !self.is_finished() || matches!(*result, JobResult::Cutting) {
            return Poll::Pending;
        }
        drop(result);

        Poll::Ready(
            self.output
                .lock()
                .take()
                .ok_or_else(|| er_new!(AktorJobError::Dropped).into()),
        )
    }
}

pub(crate) struct JobPanic {
    pub delivery: Delivery,
    pub payload: Box<dyn Any + Send>,
}

impl<W: Future<Output = ()> + Unpin> Future for Tracked<W> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        let this = self.get_mut();

        this.shared.task.register(cx.waker());

        if this.shared.aborted.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        match &mut this.work {
            Some(work) => match catch_unwind(AssertUnwindSafe(|| Pin::new(work).poll(cx))) {
                Ok(polled) => polled,
                Err(payload) => {
                    let report = crate::panic::panic_tree(&payload).er_report();
                    let admission = this.owner.admission.upgrade();
                    let delivery = this.owner.kill.record_loss(
                        AktorFailureInput::actor(
                            this.owner.name.clone(),
                            this.owner.kind,
                            AktorPhase::Background,
                            report.clone(),
                        ),
                        this.owner.starting.load(Ordering::Acquire),
                        admission.as_ref().map(|mailbox| &mailbox.lost),
                        |_| {},
                    );
                    if let Some(admission) = admission {
                        let _ = admission.terminal.set(delivery.clone());
                    }
                    {
                        let mut state = this.shared.result.lock();
                        if matches!(*state, JobResult::Running) {
                            *state = JobResult::Error(
                                AktorJobError::Failed(delivery.0),
                                report,
                                delivery.clone(),
                            );
                        }
                    }
                    std::panic::resume_unwind(Box::new(JobPanic { delivery, payload }));
                }
            },
            None => Poll::Ready(()),
        }
    }
}
impl<W> Drop for Tracked<W> {
    fn drop(&mut self) {
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(self.work.take())));
        drop(self.permit.take());
        {
            let mut result = self.shared.result.lock();
            if matches!(*result, JobResult::Running) {
                *result = JobResult::Returned;
            }
            self.shared.finished.store(true, Ordering::Release);
        }
        let wake = catch_unwind(AssertUnwindSafe(|| self.shared.joiner.wake()));

        let first = match (dropped, wake) {
            (Ok(()), Ok(())) => return,
            (Err(payload), Ok(())) | (Ok(()), Err(payload)) => payload,
            (Err(payload), Err(wake)) => Box::new(crate::panic::Failure {
                #[cfg(any(
                    feature = "tokio",
                    all(feature = "std_thread", not(target_family = "wasm"))
                ))]
                operation: None,
                payload,
                later: vec![wake],
            }),
        };

        if thread::panicking() {
            self.owner.unwinding(first);
        } else {
            std::panic::resume_unwind(first);
        }
    }
}

impl<W: Work> Spawned<W> {
    pub fn new(
        admission: Arc<Admission>,
        external: Arc<External>,
        owner: Arc<JobOwner>,
        retire: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            running: FuturesUnordered::new(),
            external,
            context: Arc::new(Context {
                owner,
                admission,
                pending: AtomicBool::new(false),
                inbox: parking_lot::Mutex::new(Vec::new()),
                retire,
            }),
        }
    }

    pub fn context(&self) -> Arc<Context<W>> {
        self.context.clone()
    }

    pub fn start(&mut self, work: W) {
        self.running.push(Running::Inside(Tracked {
            owner: self.context.owner.clone(),
            work: Some(work),
            shared: Arc::new(Shared::new(AktorJob::DropAtStop)),
            permit: None,
        }));
    }

    pub fn stop(&mut self) {
        self.collect();
        for job in self.running.iter() {
            if job.shared().policy == AktorJob::DropAtStop {
                job.shared().stop();
            }
        }
    }

    pub async fn drain(&mut self) -> Option<Box<dyn Any + Send>> {
        poll_fn(|cx| match self.poll_serve(cx) {
            Poll::Ready(payload) => Poll::Ready(Some(payload)),
            Poll::Pending if self.running.is_empty() => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        })
        .await
    }

    /// Never finishes unless spawned work panics, then it returns that panic.
    pub fn serve(&mut self) -> impl Future<Output = Box<dyn Any + Send>> + '_ {
        poll_fn(move |cx| self.poll_serve(cx))
    }

    /// Each poll starts new work and gives woken work its turn, up to a budget so a chain of
    /// ready work can't keep calls and shutdown waiting.
    pub fn poll_serve(&mut self, cx: &mut TaskContext<'_>) -> Poll<Box<dyn Any + Send>> {
        self.external.wake.register(cx.waker());
        for _ in 0..32 {
            if !self.collect() && self.running.is_empty() {
                return Poll::Pending;
            }

            let polled = catch_unwind(AssertUnwindSafe(|| self.running.poll_next_unpin(cx)));

            match polled {
                Ok(Poll::Ready(Some(()))) => continue,
                // Work spawned during those polls gets its first turn now, not on the next wake.
                Ok(Poll::Pending | Poll::Ready(None)) => {
                    if !self.collect() {
                        return Poll::Pending;
                    }
                }
                Err(payload) => return Poll::Ready(payload),
            }
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn collect(&mut self) -> bool {
        let mut added = false;
        if self.external.pending.swap(false, Ordering::Acquire) {
            let mut inbox = self.external.inbox.lock();
            added |= !inbox.is_empty();
            self.running.extend(inbox.drain(..).map(Running::Outside));
        }
        if self.context.pending.swap(false, Ordering::Acquire) {
            let mut inbox = self.context.inbox.lock();
            added |= !inbox.is_empty();
            self.running.extend(inbox.drain(..).map(Running::Inside));
        }
        added
    }
}
impl<W: Work> Drop for Spawned<W> {
    fn drop(&mut self) {
        let _visible = Visible::enter(Some(self.context.clone()));
        let _polling = Polling(POLLING.replace(true));

        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.context.admission.shutdown())) {
            self.context.owner.unwinding(payload);
        }
        self.collect();
        let running = core::mem::take(&mut self.running);
        let mut first = None;

        // One panicking drop can't keep the others from running, or abort a thread that's
        // already unwinding.
        for work in running {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
                match (&mut first, thread::panicking()) {
                    (None, false) => {
                        first = Some(crate::panic::Failure {
                            payload,
                            #[cfg(any(
                                feature = "tokio",
                                all(feature = "std_thread", not(target_family = "wasm"))
                            ))]
                            operation: None,
                            later: Vec::new(),
                        })
                    }
                    (Some(failure), false) => failure.push(payload),
                    (_, true) => self.context.owner.unwinding(payload),
                }
            }
        }

        if let Some(failure) = first {
            if failure.later.is_empty() {
                std::panic::resume_unwind(failure.payload);
            }
            std::panic::resume_unwind(Box::new(failure));
        }
    }
}

/// Polls `future` with the actor visible to `aktor_spawn`, `aktor_stopping` and `aktor_retire`.
pub fn scoped<W: Work, F: Future>(context: Arc<Context<W>>, future: F) -> Scoped<W, F> {
    Scoped {
        context: Some(context),
        stays: false,
        future,
    }
}

struct Polling(bool);
impl Drop for Polling {
    fn drop(&mut self) {
        POLLING.set(self.0);
    }
}

/// Puts an actor in `ACTIVE` and the one before back when it drops, unwinding or not.
struct Visible {
    previous: Option<Active>,
}
impl Visible {
    fn enter<W: Work>(context: Option<Arc<Context<W>>>) -> Self {
        Self {
            previous: ACTIVE.with(|active| active.replace(context.map(W::active))),
        }
    }

    fn leave<W: Work>(mut self) -> Option<Arc<Context<W>>> {
        let ours = ACTIVE.with(|active| active.replace(self.previous.take()));

        core::mem::forget(self);
        ours.and_then(W::context)
    }
}
impl Drop for Visible {
    fn drop(&mut self) {
        let previous = self.previous.take();

        let ours = ACTIVE.with(|active| active.replace(previous));
        drop(ours);
    }
}

impl<W: Work, F: Future> Future for Scoped<W, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<F::Output> {
        let this = self.project();

        // Its own thread had no actor before it, there's nothing to give back later.
        if W::STAYS {
            if let Some(context) = this.context.take() {
                let previous = ACTIVE.with(|active| active.replace(Some(W::active(context))));

                *this.stays = true;
                drop(previous);
            }

            let _polling = Polling(POLLING.replace(true));

            return this.future.poll(cx);
        }

        let visible = Visible::enter(this.context.take());
        let polled = this.future.poll(cx);

        // Moved back out instead of cloned in, so a poll costs no reference counting.
        *this.context = visible.leave();
        polled
    }
}
