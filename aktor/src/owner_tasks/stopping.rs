use super::{Admission, Arc, JobOwner};
use crate::{AktorFailureSource, AktorPhase};
use std::sync::Weak;

#[derive(Clone)]
pub struct AktorStopping {
    admission: Arc<Admission>,
    owner: Arc<JobOwner>,
}

/// Keep the guard while the blocking wait needs waking.
#[must_use]
pub struct AktorStopGuard {
    admission: Weak<Admission>,
    owner: Arc<JobOwner>,
    id: usize,
}
impl AktorStopping {
    pub(super) fn new(admission: Arc<Admission>, owner: Arc<JobOwner>) -> Self {
        Self { admission, owner }
    }

    pub fn is_stopping(&self) -> bool {
        self.admission.phase().is_closing()
    }

    pub async fn stopped(&mut self) {
        let mut changed = self.admission.watch();
        while !self.is_stopping() {
            let _ = changed.changed().await;
        }
    }

    /// Runs on the thread closing this actor, or right here if it already closed.
    pub fn on_stop(&self, callback: impl FnOnce() + Send + 'static) -> AktorStopGuard {
        let owner = self.owner.clone();
        let id = self.admission.on_stop(Box::new(move || {
            crate::group::shutdown::contain_with(callback, &owner.kill, || {
                AktorFailureSource::Actor {
                    id: (),
                    name: owner.name.clone(),
                    kind: owner.kind,
                    phase: AktorPhase::Background,
                }
            });
        }));

        AktorStopGuard {
            admission: Arc::downgrade(&self.admission),
            owner: self.owner.clone(),
            id,
        }
    }
}
impl Drop for AktorStopGuard {
    fn drop(&mut self) {
        if let Some(admission) = self.admission.upgrade() {
            crate::group::shutdown::contain_with(
                || admission.unregister(self.id),
                &self.owner.kill,
                || AktorFailureSource::Actor {
                    id: (),
                    name: self.owner.name.clone(),
                    kind: self.owner.kind,
                    phase: AktorPhase::Drop,
                },
            );
        }
    }
}
