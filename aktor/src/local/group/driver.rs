use super::*;
#[cfg(feature = "std")]
use crate::panic::{dispose_with, panic_tree};

impl<Clock: AktorGroupClock> Drop for Driver<Clock> {
    fn drop(&mut self) {
        if self.control.published.get().is_some() {
            return;
        }

        let kill = AktorKillSwitch {
            control: self.control.clone(),
            startup_failure: Vec::new(),
        };

        let loss = kill.fail(AktorFailureInput::new(
            crate::AktorFailureSource::GroupDriver,
            er_new!(DriverErr::Cancelled).er_report(),
        ));

        self.control.driver_loss.set(Some(loss));
        cancel_callers(&self.control);

        let entries = core::mem::take(&mut *self.owners.borrow_mut());

        for entry in entries.into_iter().rev() {
            let record = |error| {
                kill.owner_failure(
                    entry.id,
                    AktorFailureInput::actor(
                        entry.name.clone(),
                        entry.kind,
                        crate::AktorPhase::Drop,
                        error,
                    ),
                );
            };
            contain(entry.shutdown, record);
            contain(|| drop(entry.owner), record);

            if !entry.finished {
                let status = (entry.status)();
                let retirement = status.retirement(AktorRetirement::StillRunning);
                self.control
                    .report
                    .borrow_mut()
                    .actor_mut(entry.id)
                    .expect("registered actor")
                    .owner_outcome(OwnerOutcome {
                        retirement,
                        init: status.init,
                        ran_past_grace: false,
                    });
            }
        }

        let hook = self.control.shutdown_hook.borrow_mut().take();

        if let Some(hook) = hook {
            let snapshot = self.control.report.borrow().snapshot();
            let record = |error| {
                kill.fail(AktorFailureInput::new(
                    crate::AktorFailureSource::ShutdownCallback,
                    error,
                ));
            };
            if let Some(future) = capture(|| hook(snapshot), |_, error| record(error)) {
                let grace = self.control.grace;
                let control = Rc::downgrade(&self.control);
                let bounded = async move {
                    let mut timer =
                        core::pin::pin!(async { Clock::wait(Clock::deadline(grace)).await });
                    let mut future = future;
                    poll_fn(|cx| {
                        if let Poll::Ready(result) = future.as_mut().poll(cx) {
                            return Poll::Ready(result);
                        }
                        if timer.as_mut().poll(cx).is_ready() {
                            if let Some(control) = control.upgrade() {
                                control.report.borrow_mut().callback_timed_out();
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending
                    })
                    .await
                };
                *self.control.final_hook.borrow_mut() = Some(Box::pin(bounded));
                let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
                poll_final_hook(&self.control, &mut cx);

                if self.control.published.get().is_none() {
                    let spawner = self
                        .control
                        .spawner
                        .borrow()
                        .as_ref()
                        .and_then(alloc::rc::Weak::upgrade);
                    if let Some(spawn) = spawner {
                        let completion = AktorGroupCompletion {
                            control: self.control.clone(),
                            rejected: None,
                        };
                        let task = async move {
                            let _ = completion.wait().await;
                        };
                        if let Some(Err(error)) = capture(
                            || spawn(Box::pin(task)),
                            |_, error| {
                                kill.fail(AktorFailureInput::new(
                                    crate::AktorFailureSource::GroupDriver,
                                    error,
                                ));
                            },
                        ) {
                            kill.fail(AktorFailureInput::new(
                                crate::AktorFailureSource::GroupDriver,
                                error,
                            ));
                        }
                    }
                    self.control.changed.notify();
                }
                return;
            }
        }

        self.control.publish();
        self.control.changed.notify();
    }
}

pub(super) fn poll_final_hook<Clock: AktorGroupClock>(
    control: &Rc<Control<Clock>>,
    cx: &mut core::task::Context<'_>,
) {
    let Some(mut future) = control.final_hook.borrow_mut().take() else {
        return;
    };
    let record = |error| {
        control.report.borrow_mut().record(AktorFailureInput::new(
            crate::AktorFailureSource::ShutdownCallback,
            error,
        ));
    };
    match capture(|| future.as_mut().poll(cx), |_, error| record(error)) {
        Some(Poll::Pending) => {
            *control.final_hook.borrow_mut() = Some(future);
            return;
        }
        Some(Poll::Ready(Err((_, error)))) => record(error),
        Some(Poll::Ready(Ok(()))) | None => {}
    }
    contain(|| drop(future), record);
    control.publish();
    control.changed.notify();
}

impl OwnerStatus {
    pub fn retirement(&self, fallback: AktorRetirement) -> AktorRetirement {
        match self.cleanup_completed {
            Some(true) => AktorRetirement::Released,
            Some(false) => AktorRetirement::Terminated,
            None => fallback,
        }
    }
}

pub fn contain(action: impl FnOnce(), mut record: impl FnMut(String)) {
    capture(action, |_, error| record(error));
}

#[derive(Er)]
pub(super) enum DriverErr {
    #[er(format = "the local group driver was cancelled")]
    Cancelled,
    #[cfg(feature = "std")]
    #[er(format = "the local group driver caught a panic")]
    Panicked,
}

pub(super) fn capture<T>(
    action: impl FnOnce() -> T,
    record: impl FnMut(bool, String),
) -> Option<T> {
    #[cfg(feature = "std")]
    {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(output) => Some(output),
            Err(payload) => {
                let mut record = record;
                if !payload.is::<crate::local::panic::Reported>() {
                    record(
                        false,
                        ErBranch::from(DriverErr::Panicked)
                            .er_add(panic_tree(&payload))
                            .er_report(),
                    );
                }
                dispose_with(payload, |nested| {
                    record(true, panic_tree(nested).er_report())
                });
                None
            }
        }
    }
    #[cfg(not(feature = "std"))]
    {
        let _ = record;
        Some(action())
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

        let kill = AktorKillSwitch {
            control: control.clone(),
            startup_failure: Vec::new(),
        };
        let record = |phase, error| {
            kill.owner_failure(
                entry.id,
                AktorFailureInput::actor(entry.name.clone(), entry.kind, phase, error),
            );
        };
        let polled = capture(
            || entry.owner.as_mut().poll(cx),
            |dropped, error| {
                let phase = if dropped {
                    crate::AktorPhase::Drop
                } else {
                    crate::AktorPhase::Runner
                };
                record(phase, error);
            },
        );
        let fallback = match polled {
            Some(Poll::Pending) => continue,
            Some(Poll::Ready(error)) => {
                if let Some(error) = error {
                    record(crate::AktorPhase::Runner, error);
                }
                AktorRetirement::Released
            }
            None => {
                let owner = core::mem::replace(&mut entry.owner, Box::pin(async { None }));
                contain(
                    || drop(owner),
                    |error| record(crate::AktorPhase::Drop, error),
                );
                AktorRetirement::StillRunning
            }
        };

        entry.finished = true;
        let status = (entry.status)();
        let retirement = status.retirement(fallback);
        let mut report = control.report.borrow_mut();

        report
            .actor_mut(entry.id)
            .expect("registered actor")
            .owner_outcome(OwnerOutcome {
                retirement,
                init: status.init,
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
        drop_task(control, caller);
    }
}

fn poll_callers<Clock: AktorGroupClock>(
    control: &Rc<Control<Clock>>,
    cx: &mut core::task::Context<'_>,
) {
    let mut callers = core::mem::take(&mut *control.callers.borrow_mut());

    for caller in callers.extract_if(.., |caller| poll_task(control, caller, cx).is_ready()) {
        drop_task(control, caller);
    }

    callers.append(&mut control.callers.borrow_mut());
    *control.callers.borrow_mut() = callers;
}

pub(super) fn drop_task<Clock: AktorGroupClock>(control: &Rc<Control<Clock>>, task: Application) {
    contain(|| drop(task), |error| task_failure(control, error));
    control.applications.set(control.applications.get() - 1);
}

pub(super) fn poll_task<Clock: AktorGroupClock>(
    control: &Rc<Control<Clock>>,
    task: &mut Application,
    cx: &mut core::task::Context<'_>,
) -> Poll<()> {
    match capture(
        || task.future.as_mut().poll(cx),
        |_, error| task_failure(control, error),
    ) {
        Some(Poll::Pending) => Poll::Pending,
        Some(Poll::Ready(Err(error))) => {
            task_failure(control, error);
            Poll::Ready(())
        }
        Some(Poll::Ready(Ok(()))) | None => Poll::Ready(()),
    }
}

pub(super) fn task_failure<Clock: AktorGroupClock>(control: &Rc<Control<Clock>>, error: String) {
    let kill = AktorKillSwitch {
        control: control.clone(),
        startup_failure: Vec::new(),
    };

    kill.fail(AktorFailureInput::new(
        crate::AktorFailureSource::ApplicationTask,
        error,
    ));
}
