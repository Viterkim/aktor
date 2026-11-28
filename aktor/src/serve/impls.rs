use super::*;
use crate::queue::Admission;
use crate::{AktorFailureInput, AktorRetirement, panic::panic_tree, report::ShutdownCut};
use core::sync::atomic::Ordering;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Er)]
#[er(format = "panic during {phase}")]
pub(crate) struct ServePanic {
    phase: crate::AktorPhase,
}
impl Report {
    pub fn new(
        served: mpsc::UnboundedSender<Served>,
        name: String,
        kind: AktorExecution,
        kill: AktorKillSwitch,
        admission: Arc<Admission>,
    ) -> Self {
        Self {
            admission,
            served: Some(served),
            returned: Vec::new(),
            kill,
            name,
            kind,
            outcome: OwnerOutcome {
                retirement: AktorRetirement::StillRunning,
                init: crate::AktorInitEvidence::NeverEntered,
                ran_past_grace: false,
            },
            panic: None,
            later: Vec::new(),
            cleanup: None,
            cut: false,
            failed: false,
        }
    }

    pub fn fail_startup(
        &mut self,
        phase: crate::AktorPhase,
        message: String,
        before_stop: impl FnOnce(crate::FailureId),
    ) -> crate::Delivery {
        self.failed = true;
        self.kill.record_loss(
            self.failure(phase, message),
            true,
            Some(&self.admission.lost),
            before_stop,
        )
    }

    /// Whether the actor's latest results should stop waiting for an end that won't come clean.
    pub fn went_wrong(&self) -> bool {
        self.failed || self.outcome.ran_past_grace
    }

    fn failure(&self, phase: crate::AktorPhase, message: String) -> AktorFailureInput {
        AktorFailureInput::actor(self.name.clone(), self.kind, phase, message)
    }

    pub fn failed(&mut self, phase: crate::AktorPhase, message: String) -> crate::Delivery {
        self.failed = true;
        let cause = matches!(
            phase,
            crate::AktorPhase::Call { .. }
                | crate::AktorPhase::Interval
                | crate::AktorPhase::Runner
                | crate::AktorPhase::Background
        )
        .then_some(&self.admission.lost);
        let failure = self.failure(phase, message);
        self.kill.record_loss(failure, false, cause, |_| {})
    }

    pub fn panicked(
        &mut self,
        phase: crate::AktorPhase,
        payload: Box<dyn Any + Send>,
    ) -> crate::Delivery {
        let payload = match payload.downcast::<crate::owner_tasks::JobPanic>() {
            Ok(panic) => {
                self.failed = true;
                self.returned.push(panic.delivery.clone());
                if self.panic.is_none() {
                    self.panic = Some(panic.payload);
                } else {
                    self.dispose(panic.payload);
                }
                return panic.delivery;
            }
            Err(payload) => payload,
        };
        match payload.downcast::<crate::panic::Failure>() {
            Ok(failure) => {
                let primary = self.panicked(phase, failure.payload);
                for payload in failure.later {
                    self.panicked(crate::AktorPhase::Drop, payload);
                }
                primary
            }
            Err(payload) => self.record_panic(phase, payload),
        }
    }

    fn record_panic(
        &mut self,
        phase: crate::AktorPhase,
        payload: Box<dyn Any + Send>,
    ) -> crate::Delivery {
        let panic: ErBranch<_> = ServePanic::new(phase.clone()).into();
        let panic = panic.er_add(panic_tree(&payload));

        let delivery = self.failed(phase, panic.er_report());
        self.returned.push(delivery.clone());

        match self.panic {
            Some(_) => {
                self.later.push(panic);
                crate::panic::dispose_with(payload, |nested| {
                    self.failed(crate::AktorPhase::Drop, panic_tree(nested).er_report());
                });
            }
            None => self.panic = Some(payload),
        }
        delivery
    }

    pub fn capture<T>(
        &mut self,
        phase: crate::AktorPhase,
        action: impl FnOnce() -> T,
    ) -> Option<T> {
        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(output) => Some(output),
            Err(payload) => {
                self.panicked(phase, payload);
                None
            }
        }
    }

    pub fn dispose(&mut self, value: impl Sized) {
        self.capture(crate::AktorPhase::Drop, || drop(value));
    }

    pub fn cut(&mut self) {
        if !self.cut {
            self.cut = true;
            let receipt = self.failed(crate::AktorPhase::Runner, er_new!(ShutdownCut).er_report());
            self.returned.push(receipt);
        }
    }

    pub fn finish(mut self) {
        self.send(None);
    }

    fn send(&mut self, owner_loss: Option<crate::FailureId>) {
        let Some(served) = self.served.take() else {
            return;
        };

        let served = served.send(Served {
            returned: core::mem::take(&mut self.returned),
            owner_loss,
            outcome: self.outcome.clone(),
            panic: self.panic.take(),
            later: core::mem::take(&mut self.later),
            cleanup: self.cleanup.take(),
            cut: self.cut,
        });

        // Nobody listens anymore, a panic payload in there can still panic as it drops.
        if let Err(served) = served
            && let Some(payload) = served.0.panic
        {
            crate::panic::dispose_with(payload, |nested| {
                self.failed(crate::AktorPhase::Drop, panic_tree(nested).er_report());
            });
        }
    }
}
#[derive(Er)]
#[er(display = "actor task cancelled before cleanup completed")]
struct OwnerGone;
impl Drop for Report {
    fn drop(&mut self) {
        if self.served.is_none() {
            return;
        }

        self.outcome.retirement = AktorRetirement::StillRunning;

        let owner_loss = if std::thread::panicking() {
            None
        } else {
            Some(
                self.kill
                    .fail(self.failure(crate::AktorPhase::Runner, er_new!(OwnerGone).er_report())),
            )
        };
        self.send(owner_loss);
    }
}

impl Due {
    pub(super) fn new() -> (Self, Arc<DueState>) {
        let state = Arc::new(DueState {
            done: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        });

        (Self(state.clone()), state)
    }
}
impl Drop for Due {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
        self.0.waker.wake();
    }
}

impl DueState {
    pub fn done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}
