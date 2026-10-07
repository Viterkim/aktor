use super::*;
use alloc::string::ToString;

impl<Clock: AktorGroupClock> Drop for Driver<Clock> {
    fn drop(&mut self) {
        if self.control.completed.borrow().is_some() {
            return;
        }

        let kill = KillSwitch {
            control: self.control.clone(),
            startup_failure: None,
        };

        kill.fail(ActorFailure {
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

            if !entry.finished {
                self.control.report.borrow_mut().actors.push(ActorOutcome {
                    kind: Some(entry.kind),
                    actor: entry.name,
                    diagnostics: alloc::vec![AktorError::new(
                        "actor owner cancelled before cleanup finished"
                    )],
                    timed_out: false,
                });
            }

            if let Some(error) = contain(|| drop(entry.owner)) {
                self.control.report.borrow_mut().application.push(error);
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
                let message = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).into()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "panic payload had no message".into()
                };

                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                    core::mem::forget(payload);
                }

                Err(AktorError::new(message))
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

        let error = match capture(|| entry.owner.as_mut().poll(cx)) {
            Ok(Poll::Pending) => continue,
            Ok(Poll::Ready(error)) => error,
            Err(error) => {
                KillSwitch {
                    control: control.clone(),
                    startup_failure: None,
                }
                .fail(ActorFailure {
                    kind: Some(entry.kind),
                    actor: entry.name.clone(),
                    phase: "owner".into(),
                    message: error.to_string(),
                });

                let owner = core::mem::replace(&mut entry.owner, Box::pin(async { None }));

                if let Some(secondary) = contain(|| drop(owner)) {
                    control.report.borrow_mut().application.push(secondary);
                }

                Some(error)
            }
        };

        entry.finished = true;

        let mut diagnostics = core::mem::take(&mut *entry.diagnostics.borrow_mut());

        diagnostics.extend(error);
        control.report.borrow_mut().actors.push(ActorOutcome {
            kind: Some(entry.kind),
            actor: entry.name.clone(),
            diagnostics,
            timed_out: false,
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
    let callers = core::mem::take(&mut *control.callers.borrow_mut());
    let mut waiting = Vec::new();

    for mut caller in callers {
        let pending = if control.stopping.get() {
            false
        } else {
            match capture(|| caller.as_mut().poll(cx)) {
                Ok(Poll::Pending) => true,
                Ok(Poll::Ready(())) => false,
                Err(error) => {
                    task_failure(control, "task", error);
                    false
                }
            }
        };

        if pending && !control.stopping.get() {
            waiting.push(caller);
        } else if let Some(error) = contain(|| drop(caller)) {
            task_failure(control, "task drop", error);
        }
    }

    let registered = core::mem::take(&mut *control.callers.borrow_mut());

    waiting.extend(registered);
    *control.callers.borrow_mut() = waiting;

    if control.stopping.get() {
        cancel_callers(control);
    }
}

fn task_failure<Clock: AktorGroupClock>(
    control: &Rc<Control<Clock>>,
    phase: &str,
    error: AktorError,
) {
    let kill = KillSwitch {
        control: control.clone(),
        startup_failure: None,
    };

    kill.fail(ActorFailure {
        kind: None,
        actor: "application".into(),
        phase: phase.into(),
        message: error.to_string(),
    });
    kill.record_diagnostic(error);
}
