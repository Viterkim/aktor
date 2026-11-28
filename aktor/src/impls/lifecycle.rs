use crate::types::output::CleanupResult;
use crate::{
    ActorId, AktorFailure, AktorFailureInput, AktorFailureSource, AktorOutcome, AktorPhase,
    FailureId,
};
use crate::{
    AktorCleanupError, AktorCleanupOutput, AktorExecution, AktorRetirement, AktorShutdownAsync,
    AktorShutdownFuture, AktorShutdownOutput, AktorShutdownReport, AktorShutdownSync,
};
use alloc::string::String;
use core::{
    fmt,
    future::{Future, Ready, ready},
    pin::Pin,
    task::{Context, Poll},
};
use er::*;

impl AktorCleanupOutput<()> for () {
    fn into_cleanup(self) -> CleanupResult {
        Ok(())
    }
}
impl<E, X: Into<(E, String)>> AktorCleanupOutput<E> for Result<(), X> {
    fn into_cleanup(self) -> CleanupResult {
        match self {
            Ok(()) => Ok(()),
            Err(error) => {
                let (_, report) = error.into();
                Err(crate::report::with_report(AktorCleanupError, report))
            }
        }
    }
}
impl<T: AktorCleanupOutput<E>, E> AktorShutdownOutput<(AktorShutdownSync, E)> for T {
    type Future = Ready<CleanupResult>;

    fn into_shutdown(self) -> Self::Future {
        ready(self.into_cleanup())
    }
}
impl<F, E> AktorShutdownOutput<(AktorShutdownAsync, E)> for F
where
    F: Future,
    F::Output: AktorCleanupOutput<E>,
{
    type Future = AktorShutdownFuture<F>;

    fn into_shutdown(self) -> Self::Future {
        AktorShutdownFuture {
            future: alloc::boxed::Box::pin(self),
            convert: F::Output::into_cleanup,
        }
    }
}
impl<F: Future> Future for AktorShutdownFuture<F> {
    type Output = CleanupResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.future.as_mut().poll(cx).map(this.convert)
    }
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    feature = "wasm_browser_workers",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl crate::AktorRetirementError {
    pub(crate) fn unconfirmed(report: Option<AktorShutdownReport>) -> (Self, String) {
        let text = report.as_ref().map(|r| r.render(None)).unwrap_or_default();
        let error = Self {
            report: report.map(alloc::boxed::Box::new),
        };

        crate::report::with_report(error, text)
    }
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl crate::AktorRetiredStatus {
    pub(crate) fn from_report(report: &AktorShutdownReport) -> Self {
        use crate::{AktorRetired, AktorRetirement::*};
        match report.retirement() {
            Some(Released) => Self::Confirmed(AktorRetired::Released(report.clone())),
            Some(Terminated) => Self::Confirmed(AktorRetired::Terminated(report.clone())),
            Some(StillRunning) | None => Self::Unconfirmed(Some(report.clone())),
        }
    }

    pub(crate) fn into_result(
        self,
    ) -> Option<Result<crate::AktorRetired, (crate::AktorRetirementError, String)>> {
        match self {
            Self::Pending => None,
            Self::Confirmed(retired) => Some(Ok(retired)),
            Self::Unconfirmed(report) => {
                Some(Err(crate::AktorRetirementError::unconfirmed(report)))
            }
        }
    }
}

impl AktorFailure {
    /// Whether a caller has received this failure, even since the report was published.
    pub fn delivered(&self) -> bool {
        self.delivery.received()
    }
}

impl AktorFailureInput {
    pub fn actor(name: String, kind: AktorExecution, phase: AktorPhase, error: String) -> Self {
        Self::new(
            AktorFailureSource::Actor {
                id: (),
                name,
                kind,
                phase,
            },
            error,
        )
    }

    pub fn new(source: AktorFailureSource<()>, error: String) -> Self {
        Self { source, error }
    }
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    feature = "wasm_browser_workers",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl crate::AktorInitEvidence {
    pub(crate) fn observed(self, evidence: Self) -> Self {
        use crate::AktorInitEvidence::*;
        match (self, evidence) {
            (Ready, _) | (_, Ready) => Ready,
            (Entered, _) | (_, Entered) => Entered,
            (NeverEntered, _) | (_, NeverEntered) => NeverEntered,
            _ => Unknown,
        }
    }
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    feature = "wasm_browser_workers",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl AktorOutcome {
    pub(crate) fn owner_outcome(&mut self, outcome: crate::OwnerOutcome) {
        self.init = self.init.observed(outcome.init);
        self.owner = Some(crate::AktorOwnerOutcome {
            retirement: outcome.retirement,
            ran_past_grace: self.ran_past_grace() || outcome.ran_past_grace,
        });
    }
}

impl AktorOutcome {
    pub fn retirement(&self) -> Option<AktorRetirement> {
        self.owner.map(|owner| owner.retirement)
    }

    pub fn ran_past_grace(&self) -> bool {
        self.owner.is_some_and(|owner| owner.ran_past_grace)
    }
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl ActorId {
    pub(crate) fn next() -> Self {
        use portable_atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(
            NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .expect("actor IDs exhausted"),
        )
    }
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl crate::Recorder {
    pub(crate) fn snapshot(&self) -> AktorShutdownReport {
        AktorShutdownReport {
            stop_cause: self.stop_cause.expect("shutdown has started"),
            actors: self.actors.clone(),
            failures: self.failures.clone(),
            late_work: self.late_work.clone(),
        }
    }

    pub(crate) fn startup(&self, scope: Option<ActorId>) -> crate::AktorStartup {
        let actors = self
            .actors
            .iter()
            .filter(|actor| {
                scope.is_some_and(|scope| {
                    self.scopes
                        .iter()
                        .any(|member| member.root == scope && member.id == actor.id)
                })
            })
            .map(|actor| crate::AktorActorInit {
                id: actor.id,
                name: actor.name.clone(),
                kind: actor.kind,
                init: actor.init,
            })
            .collect();

        crate::AktorStartup { actors }
    }

    pub(crate) fn child(&self, parent: ActorId, index: usize) -> Option<ActorId> {
        self.scopes
            .iter()
            .filter(|scope| scope.parent == Some(parent))
            .nth(index)
            .map(|scope| scope.id)
    }

    #[cfg(any(
        feature = "local",
        feature = "transport",
        all(target_family = "wasm", feature = "wasm_browser_workers")
    ))]
    pub(crate) fn failure(&self, id: FailureId) -> Option<&AktorFailure> {
        self.failures.iter().find(|failure| failure.id == id)
    }

    pub(crate) fn late(&mut self, work: crate::AktorLateWork) {
        if !self.late_work.contains(&work) {
            self.late_work.push(work);
        }
    }
    pub(crate) fn actor_mut(&mut self, id: ActorId) -> Option<&mut AktorOutcome> {
        self.actors.iter_mut().find(|actor| actor.id == id)
    }

    pub(crate) fn attempt(
        &mut self,
        id: ActorId,
        name: &str,
        kind: AktorExecution,
        init: crate::AktorInitEvidence,
    ) -> &mut AktorOutcome {
        if let Some(index) = self.actors.iter().position(|actor| actor.id == id) {
            return &mut self.actors[index];
        }

        self.actors.push_mut(AktorOutcome {
            id,
            name: name.into(),
            kind,
            init,
            owner: None,
        })
    }

    pub(crate) fn record_for(
        &mut self,
        actor: Option<crate::ActorId>,
        failure: AktorFailureInput,
    ) -> crate::Delivery {
        let id = FailureId::next();
        let source = match failure.source {
            AktorFailureSource::Actor {
                name, kind, phase, ..
            } => {
                let id = actor.unwrap_or_else(ActorId::next);
                self.attempt(id, &name, kind, crate::AktorInitEvidence::Unknown);
                AktorFailureSource::Actor {
                    id,
                    name,
                    kind,
                    phase,
                }
            }
            AktorFailureSource::GroupDriver => AktorFailureSource::GroupDriver,
            AktorFailureSource::ApplicationTask => AktorFailureSource::ApplicationTask,
            AktorFailureSource::ShutdownCallback => AktorFailureSource::ShutdownCallback,
        };
        let delivery = crate::Delivery::new(id, failure.error.clone());
        self.failures.push(AktorFailure {
            id,
            delivery: delivery.clone(),
            source,
            error: failure.error,
        });
        delivery
    }

    pub(crate) fn record(&mut self, failure: AktorFailureInput) -> crate::FailureId {
        self.record_for(None, failure).0
    }

    pub(crate) fn callback_timed_out(&mut self) {
        #[derive(Er)]
        #[er(format = "the group shutdown callback was cancelled at its deadline")]
        struct CallbackTimedOut;

        self.record(AktorFailureInput::new(
            AktorFailureSource::ShutdownCallback,
            er_new!(CallbackTimedOut).er_report(),
        ));
    }
}

impl AktorShutdownReport {
    pub fn actor(&self, id: ActorId) -> Option<&AktorOutcome> {
        self.actors.iter().find(|actor| actor.id == id)
    }

    pub fn failure(&self, id: FailureId) -> Option<&AktorFailure> {
        self.failures.iter().find(|failure| failure.id == id)
    }

    pub fn actor_failures(&self, id: ActorId) -> impl Iterator<Item = &AktorFailure> {
        self.failures.iter().filter(move |failure| matches!(failure.source, AktorFailureSource::Actor { id: actor, .. } if actor == id))
    }

    pub fn ran_past_grace(&self) -> bool {
        !self.late_work.is_empty() || self.actors.iter().any(AktorOutcome::ran_past_grace)
    }

    /// Returns the error and printed report when [`failed()`](Self::failed) is true. A clean shutdown returns `Ok(())` even past grace.
    ///
    /// `started.stop().await.into_result()?;`
    pub fn into_result(self) -> Result<(), (crate::AktorShutdownError, String)> {
        if !self.failed() {
            return Ok(());
        }

        let text = self.render(None);

        Err((
            crate::AktorShutdownError {
                report: alloc::boxed::Box::new(self),
            },
            text,
        ))
    }

    /// True when a failure stopped the group or someone still needs to handle a failure.
    pub fn failed(&self) -> bool {
        matches!(
            self.stop_cause,
            crate::StopCause::Failure(_) | crate::StopCause::StartupFailure(_)
        ) || self.failures.iter().any(|failure| !failure.delivered())
    }

    /// Every reported actor has stopped and let go of its resources. Returns false when no actor
    /// acquired an owner.
    pub fn released(&self) -> bool {
        matches!(
            self.retirement(),
            Some(AktorRetirement::Released | AktorRetirement::Terminated)
        )
    }

    /// Combined retirement of the acquired owners. An empty group has no owner evidence.
    pub fn retirement(&self) -> Option<AktorRetirement> {
        let mut status = None;
        for retirement in self.actors.iter().filter_map(AktorOutcome::retirement) {
            match retirement {
                AktorRetirement::StillRunning => return Some(retirement),
                AktorRetirement::Terminated => status = Some(retirement),
                AktorRetirement::Released => {
                    status.get_or_insert(retirement);
                }
            }
        }

        status
    }
}
// The report is an er tree: aktor's own lines are its nodes, and a report that came from somewhere
// else (an actor's `end`, a panic, a worker) is saved under the line it belongs to. That way it
// prints like any other er report, and nests properly when an app saves this one in turn.
impl fmt::Display for AktorShutdownReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let branch = self.summary(None);
        if branch.nodes.is_empty() {
            Ok(())
        } else {
            branch.er_write_report(f)
        }
    }
}

impl AktorShutdownReport {
    pub(crate) fn render(&self, skip: Option<crate::FailureId>) -> String {
        let branch = self.summary(skip);
        if branch.nodes.is_empty() {
            String::new()
        } else {
            branch.er_report()
        }
    }

    fn summary(&self, skip: Option<crate::FailureId>) -> ErBranch<Shutdown> {
        let mut failed = matches!(self.stop_cause, crate::StopCause::Failure(id) | crate::StopCause::StartupFailure(id) if Some(id) != skip);
        let mut report: ErBranch<_> = Shutdown::Slow.into();

        for failure in &self.failures {
            if Some(failure.id) == skip {
                continue;
            }

            let row = if failure.delivered() {
                FailureRow::Delivered(failure.source.clone())
            } else {
                failed = true;
                FailureRow::Undelivered(failure.source.clone())
            };
            report = report.er_add(row.er_save("report", failure.error.clone()));
        }

        for actor in &self.actors {
            if let Some(retirement) = actor
                .retirement()
                .filter(|retirement| *retirement != AktorRetirement::Released)
            {
                report = report.er_add(ActorNotReleased {
                    actor: actor.name.clone(),
                    retirement,
                });
            }

            if actor.ran_past_grace() {
                report = report.er_add(ActorPastGrace {
                    actor: actor.name.clone(),
                    kind: actor.kind,
                });
            }
        }

        for work in &self.late_work {
            report = report.er_add(LateWork(*work));
        }

        if failed {
            report.value = Shutdown::Failed;
        }
        report
    }
}

#[derive(Er)]
enum Shutdown {
    #[er(format = "actor group shut down with failures")]
    Failed,
    #[er(format = "actor group shut down")]
    Slow,
}

#[derive(Er)]
enum FailureRow {
    #[er(format = "{0} (delivered)")]
    Delivered(AktorFailureSource),
    #[er(format = "{0}")]
    Undelivered(AktorFailureSource),
}

#[derive(Er)]
#[er(format = "{actor:?} {retirement}")]
struct ActorNotReleased {
    actor: String,
    retirement: AktorRetirement,
}

#[derive(Er)]
#[er(format = "{actor:?} ({kind:?}) ran past its grace")]
struct ActorPastGrace {
    actor: String,
    kind: AktorExecution,
}

#[derive(Er)]
#[er(format = "{0}")]
struct LateWork(crate::AktorLateWork);
