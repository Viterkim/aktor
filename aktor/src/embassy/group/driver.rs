use super::*;

impl Drop for Driver {
    fn drop(&mut self) {
        if self.control.completed.borrow().is_some() {
            return;
        }
        let kill = KillSwitch {
            control: self.control.clone(),
        };
        kill.fail(ActorFailure {
            actor: "application".into(),
            phase: "driver".into(),
            message: "actor group driver cancelled".into(),
        });
        let entries = core::mem::take(&mut *self.owners.borrow_mut());
        for entry in entries.into_iter().rev() {
            (entry.shutdown)();
            if !entry.finished {
                self.control.report.borrow_mut().actors.push(ActorOutcome {
                    actor: entry.name,
                    diagnostics: alloc::vec![AktorError::new(
                        "actor owner cancelled before cleanup finished"
                    )],
                    timed_out: false,
                });
            }
            drop(entry.owner);
        }
        let mut report = self.control.report.borrow().clone();
        report.failure = self.control.failure.borrow().clone();
        *self.control.completed.borrow_mut() = Some(report);
        self.control.changed.notify();
    }
}

pub fn poll_owners(
    owners: &RefCell<Vec<Entry>>,
    cx: &mut core::task::Context<'_>,
    control: &Control,
) {
    let mut entries = core::mem::take(&mut *owners.borrow_mut());
    for entry in entries.iter_mut() {
        if !entry.finished
            && let Poll::Ready(error) = entry.owner.as_mut().poll(cx)
        {
            entry.finished = true;
            control.report.borrow_mut().actors.push(ActorOutcome {
                actor: entry.name.clone(),
                diagnostics: error.into_iter().collect(),
                timed_out: false,
            });
        }
    }
    let registered = core::mem::take(&mut *owners.borrow_mut());
    entries.extend(registered);
    *owners.borrow_mut() = entries;
}
