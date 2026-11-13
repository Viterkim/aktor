use super::*;
#[cfg(feature = "std")]
use crate::panic::{dispose_secondary, panic_message};

impl<Clock: AktorGroupClock> Drop for Driver<Clock> {
    fn drop(&mut self) {
        if self.control.completed.borrow().is_some() {
            return;
        }

        let kill = AktorKillSwitch {
            control: self.control.clone(),
            startup_failure: None,
        };

        kill.fail(AktorFailureReport {
            kind: None,
            actor: "application".into(),
            phase: "driver".into(),
            message: "actor group driver cancelled".into(),
        });

        cancel_callers(&self.control);

        let entries = core::mem::take(&mut *self.owners.borrow_mut());

        for entry in entries.into_iter().rev() {
            if let Some(error) = contain(entry.shutdown) {
                self.control.report.borrow_mut().application.push(error);
            }

            if let Some(error) = contain(|| drop(entry.owner)) {
                self.control.report.borrow_mut().application.push(error);
            }

            if !entry.finished {
                let status = (entry.status)();
                let retirement = status.retirement(AktorRetireStatus::Unconfirmed);
                let mut diagnostics = core::mem::take(&mut *entry.diagnostics.borrow_mut());
                diagnostics.extend(status.cleanup_diagnostic);
                diagnostics.push(AktorError::new(
                    "actor owner cancelled before cleanup finished",
                    (),
                ));

                self.control.report.borrow_mut().actors.push(AktorOutcome {
                    retirement,
                    kind: entry.kind,
                    actor: entry.name,
                    diagnostics,
                    ran_past_grace: false,
                });
            }
        }

        let hook = self.control.shutdown_hook.borrow_mut().take();

        if let Some(error) = contain(|| drop(hook)) {
            self.control.report.borrow_mut().application.push(error);
        }

        let mut report = self.control.report.borrow().clone();

        report.failure = self.control.failure.borrow().clone();
        *self.control.completed.borrow_mut() = Some(report);
        self.control.changed.notify();
    }
}

impl OwnerStatus {
    pub fn retirement(&self, fallback: AktorRetireStatus) -> AktorRetireStatus {
        match self.cleanup_completed {
            Some(true) => AktorRetireStatus::Confirmed,
            Some(false) => AktorRetireStatus::Terminated,
            None => fallback,
        }
    }
}

pub fn contain(action: impl FnOnce()) -> Option<AktorError> {
    capture(action).err()
}

pub fn capture<T>(action: impl FnOnce() -> T) -> Result<T, AktorError> {
    #[cfg(feature = "std")]
    {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(output) => Ok(output),
            Err(payload) => {
                let message = panic_message(&payload);

                dispose_secondary(payload);
                Err(AktorError::new(message, ()))
            }
        }
    }
    #[cfg(not(feature = "std"))]
    {
        Ok(action())
    }
}

pub fn poll_owners<Clock: AktorGroupClock>(
    owners: &RefCell<Vec<Entry>>,
    cx: &mut core::task::Context<'_>,
    control: &Rc<Control<Clock>>,
) {
    poll_callers(control, cx);

    let mut entries = core::mem::take(&mut *owners.borrow_mut());

    for entry in entries.iter_mut() {
        if entry.finished {
            continue;
        }

        let result = match capture(|| entry.owner.as_mut().poll(cx)) {
            Ok(Poll::Pending) => continue,
            Ok(Poll::Ready(error)) => Ok(error),
            Err(error) => Err(error),
        };
        let mut status = (entry.status)();

        let (error, fallback) = match result {
            Ok(error) => (
                status.cleanup_diagnostic.take().or(error),
                AktorRetireStatus::Confirmed,
            ),
            Err(error) => {
                // The owner reports its own panic before passing it on, don't print it twice.
                let reported = status.panic_reported
                    || AktorKillSwitch {
                        control: control.clone(),
                        startup_failure: None,
                    }
                    .fail(AktorFailureReport {
                        kind: Some(entry.kind),
                        actor: entry.name.clone(),
                        phase: "owner".into(),
                        message: error.diagnostics.clone(),
                    });

                let owner = core::mem::replace(&mut entry.owner, Box::pin(async { None }));

                if let Some(secondary) = contain(|| drop(owner)) {
                    control.report.borrow_mut().application.push(secondary);
                }

                ((!reported).then_some(error), AktorRetireStatus::Unconfirmed)
            }
        };

        entry.finished = true;

        let mut diagnostics = core::mem::take(&mut *entry.diagnostics.borrow_mut());

        let retirement = status.retirement(fallback);
        diagnostics.extend(status.cleanup_diagnostic);
        diagnostics.extend(error);

        let mut report = control.report.borrow_mut();
        report.ran_past_grace |= status.ran_past_grace;
        report.actors.push(AktorOutcome {
            retirement,
            kind: entry.kind,
            actor: entry.name.clone(),
            diagnostics,
            ran_past_grace: status.ran_past_grace,
        });
    }

    let registered = core::mem::take(&mut *owners.borrow_mut());

    entries.extend(registered);
    *owners.borrow_mut() = entries;
}

pub fn cancel_callers<Clock: AktorGroupClock>(control: &Rc<Control<Clock>>) {
    let callers = core::mem::take(&mut *control.callers.borrow_mut());

    for caller in callers {
        if let Some(error) = contain(|| drop(caller)) {
            task_failure(control, "task drop", error);
        }
    }
}

fn poll_callers<Clock: AktorGroupClock>(
    control: &Rc<Control<Clock>>,
    cx: &mut core::task::Context<'_>,
) {
    let mut callers = core::mem::take(&mut *control.callers.borrow_mut());

    for caller in callers.extract_if(.., |caller| {
        if control.stopping.get() {
            return true;
        }

        match capture(|| caller.as_mut().poll(cx)) {
            Ok(Poll::Pending) => control.stopping.get(),
            Ok(Poll::Ready(())) => true,
            Err(error) => {
                task_failure(control, "task", error);
                true
            }
        }
    }) {
        if let Some(error) = contain(|| drop(caller)) {
            task_failure(control, "task drop", error);
        }
    }

    callers.append(&mut control.callers.borrow_mut());
    *control.callers.borrow_mut() = callers;

    if control.stopping.get() {
        cancel_callers(control);
    }
}

fn task_failure<Clock: AktorGroupClock>(
    control: &Rc<Control<Clock>>,
    phase: &str,
    error: AktorError,
) {
    let kill = AktorKillSwitch {
        control: control.clone(),
        startup_failure: None,
    };

    let primary = kill.fail(AktorFailureReport {
        kind: None,
        actor: "application".into(),
        phase: phase.into(),
        message: error.diagnostics.clone(),
    });
    if !primary {
        kill.record_diagnostic(error);
    }
}
