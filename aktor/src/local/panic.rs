use super::*;
use crate::panic::{PanicErr, dispose_with, panic_tree};
use core::{future::poll_fn, pin::Pin, task::Poll};
use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
};

pub(crate) fn panic_report(payload: &Box<dyn Any + Send>) -> String {
    panic_tree(payload).er_report()
}

type Payload = Box<dyn Any + Send>;

pub(crate) struct Reported(pub Payload, pub Option<crate::FailureId>);

#[derive(Er)]
#[er(format = "panic during {phase}")]
struct During {
    phase: crate::AktorPhase,
}

pub(crate) async fn original<F: Future>(future: F) -> F::Output {
    let mut future = core::pin::pin!(future);
    let result =
        poll_fn(
            |cx| match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(poll) => poll.map(Ok),
                Err(payload) => Poll::Ready(Err(payload)),
            },
        )
        .await;

    match result {
        Ok(result) => result,
        Err(payload) => match payload.downcast::<Reported>() {
            Ok(reported) => panic::resume_unwind(reported.0),
            Err(payload) => panic::resume_unwind(payload),
        },
    }
}

impl<S> Inner<S> {
    pub(super) fn resume(&self, payload: Payload) -> ! {
        if self.group.borrow().is_some() {
            panic::resume_unwind(Box::new(Reported(payload, None)))
        }
        panic::resume_unwind(payload)
    }

    pub async fn settle<F: Future>(
        &self,
        phase: crate::AktorPhase,
        future: F,
        primary: &mut Option<Box<dyn Any + Send>>,
    ) -> Result<F::Output, ErBranch<PanicErr>> {
        match self
            .capture_result(|| phase.clone(), Box::pin(future))
            .await
        {
            Ok(value) => Ok(value),
            Err(Reported(payload, id)) => {
                self.remember(id);
                let error = panic_tree(&payload);

                if primary.is_none() {
                    *primary = Some(payload);
                } else {
                    self.dispose_panic(payload);
                }

                Err(error)
            }
        }
    }

    pub async fn capture<F: Future + ?Sized>(
        &self,
        phase: impl Fn() -> crate::AktorPhase,
        future: Pin<Box<F>>,
    ) -> F::Output {
        match self.capture_result(phase, future).await {
            Ok(output) => output,
            Err(reported) if self.group.borrow().is_some() => {
                panic::resume_unwind(Box::new(reported))
            }
            Err(reported) => panic::resume_unwind(reported.0),
        }
    }

    pub async fn capture_result<F: Future + ?Sized>(
        &self,
        phase: impl Fn() -> crate::AktorPhase,
        future: Pin<Box<F>>,
    ) -> Result<F::Output, Reported> {
        let id = Cell::new(None);
        match catch_pinned(future, |dropped, payload, secondary| {
            if !payload.is::<Reported>() {
                let recorded = self.report_panic(
                    if dropped {
                        crate::AktorPhase::Drop
                    } else {
                        phase()
                    },
                    payload,
                    secondary,
                );
                if !secondary {
                    if matches!(
                        phase(),
                        crate::AktorPhase::Start
                            | crate::AktorPhase::Runner
                            | crate::AktorPhase::Call { .. }
                            | crate::AktorPhase::Interval
                    ) {
                        self.lost(recorded);
                    } else {
                        self.remember(recorded);
                    }
                    id.set(recorded);
                }
            }
        })
        .await
        {
            Ok(output) => Ok(output),
            Err(payload) => match payload.downcast::<Reported>() {
                Ok(reported) => Err(*reported),
                Err(original) => Err(Reported(original, id.get())),
            },
        }
    }

    pub(super) fn dispose_panic(&self, payload: Payload) {
        dispose_with(payload, |payload| {
            self.report_panic(crate::AktorPhase::Drop, payload, true);
        });
    }

    pub(super) fn report_panic(
        &self,
        phase: crate::AktorPhase,
        payload: &Payload,
        secondary: bool,
    ) -> Option<crate::FailureId> {
        if !secondary || self.group.borrow().is_some() {
            return self.record(phase, panic_report(payload));
        }
        self.save_panic(phase, payload);
        None
    }

    pub(super) fn save_panic(&self, phase: crate::AktorPhase, payload: &Payload) {
        if self.group.borrow().is_some() {
            self.record(phase, panic_report(payload));
            return;
        }
        let diagnostic = ErBranch::from(During::new(phase))
            .er_add(panic_tree(payload))
            .er_report();
        self.completion.diagnostics.borrow_mut().push(diagnostic);
    }
}

pub async fn catch<F: Future>(
    future: F,
    report: impl Fn(bool, &Box<dyn Any + Send>),
) -> Result<F::Output, Box<dyn Any + Send>> {
    catch_pinned(Box::pin(future), |dropped, payload, _| {
        report(dropped, payload)
    })
    .await
}

pub(crate) async fn catch_pinned<F: Future + ?Sized>(
    mut future: Pin<Box<F>>,
    report: impl Fn(bool, &Payload, bool),
) -> Result<F::Output, Payload> {
    let result =
        poll_fn(
            |cx| match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(result) => result.map(Ok),
                Err(payload) => Poll::Ready(Err(payload)),
            },
        )
        .await;

    if let Err(payload) = &result {
        report(false, payload, false);
    }

    let discarded = panic::catch_unwind(AssertUnwindSafe(|| drop(future)));

    let secondary = |payload| {
        report(true, &payload, true);
        dispose_with(payload, |nested| report(true, nested, true));
    };

    match (result, discarded) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(payload), discarded) => {
            if let Err(dropped) = discarded {
                secondary(dropped);
            }

            Err(payload)
        }
        (Ok(output), Err(payload)) => {
            report(true, &payload, false);

            if let Err(dropped) = panic::catch_unwind(AssertUnwindSafe(|| drop(output))) {
                secondary(dropped);
            }

            Err(payload)
        }
    }
}

pub fn discard<T>(
    values: impl IntoIterator<Item = T>,
    secondary: impl FnMut(&Box<dyn Any + Send>),
) -> Option<Box<dyn Any + Send>> {
    capture_each(values, drop, secondary)
}

pub fn capture_each<T>(
    values: impl IntoIterator<Item = T>,
    mut action: impl FnMut(T),
    mut secondary: impl FnMut(&Box<dyn Any + Send>),
) -> Option<Box<dyn Any + Send>> {
    let mut primary = None;

    for value in values {
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| action(value))) {
            if primary.is_none() {
                primary = Some(payload);
            } else {
                secondary(&payload);
                dispose_with(payload, &mut secondary);
            }
        }
    }

    primary
}

pub fn discard_hooks<S>(
    hooks: super::hooks::CallHooks<S>,
    mut secondary: impl FnMut(&Box<dyn Any + Send>),
) -> Option<Box<dyn Any + Send>> {
    let mut primary = discard([hooks.before_each, hooks.after_each], &mut secondary);

    if let Some(payload) = discard(hooks.intervals, &mut secondary) {
        if primary.is_none() {
            primary = Some(payload);
        } else {
            secondary(&payload);
            dispose_with(payload, &mut secondary);
        }
    }

    primary
}
