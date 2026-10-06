use super::*;
use alloc::string::ToString;

impl<Clock: AktorGroupClock> Drop for Driver<Clock> {
    fn drop(&mut self) {
        if self.control.completed.borrow().is_some() {
            return;
        }

        let kill = KillSwitch {
            control: self.control.clone(),
        };

        kill.fail(ActorFailure {
            kind: None,
            actor: "application".into(),
            phase: "driver".into(),
            message: "actor group driver cancelled".into(),
        });

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
    #[cfg(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    ))]
    {
        use crate::{group::shutdown::panic_message, listener::failure::dispose_secondary};
        use std::panic::{AssertUnwindSafe, catch_unwind};

        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(output) => Ok(output),
            Err(payload) => {
                let message = panic_message(&payload);
                dispose_secondary(payload);
                Err(AktorError::new(message))
            }
        }
    }
    #[cfg(not(any(
        feature = "tokio",
        all(feature = "std_thread", not(target_family = "wasm"))
    )))]
    {
        Ok(action())
    }
}

pub fn poll_owners<Clock: AktorGroupClock>(
    owners: &RefCell<Vec<Entry>>,
    cx: &mut core::task::Context<'_>,
    control: &Rc<Control<Clock>>,
) {
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
