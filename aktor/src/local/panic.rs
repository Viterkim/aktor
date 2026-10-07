use super::*;
use crate::panic::{dispose_secondary, panic_message};
use core::{future::poll_fn, task::Poll};
use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
};

impl<S, const N: usize, E> Inner<S, N, E> {
    pub async fn settle<F: Future>(
        &self,
        phase: &str,
        future: F,
        primary: &mut Option<Box<dyn Any + Send>>,
    ) -> Result<F::Output, crate::AktorError> {
        self.capture_result(phase, future).await.map_err(|payload| {
            let error = crate::AktorError::new(panic_message(&payload));

            if primary.is_none() {
                *primary = Some(payload);
            } else {
                dispose_secondary(payload);
            }

            error
        })
    }

    pub async fn capture<F: Future>(&self, phase: &str, future: F) -> F::Output {
        match self.capture_result(phase, future).await {
            Ok(output) => output,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    pub async fn capture_result<F: Future>(
        &self,
        phase: &str,
        future: F,
    ) -> Result<F::Output, Box<dyn Any + Send>> {
        catch(future, |dropping, payload| {
            self.report_panic(if dropping { "future drop" } else { phase }, payload);
        })
        .await
    }

    fn report_panic(&self, phase: &str, payload: &Box<dyn Any + Send>) {
        let supervisor = self.group.borrow().clone();

        if let Some((name, group)) = supervisor {
            if self.completion.panic_reported.replace(true) {
                return;
            }

            let message = panic_message(payload);
            let failure = crate::ActorFailure {
                kind: None,
                actor: name,
                phase: phase.into(),
                message: message.clone(),
            };

            let first = if self.completion.ready.borrow().is_none() {
                group.fail_startup(failure)
            } else {
                group.fail(failure)
            };

            if !first {
                self.completion
                    .diagnostics
                    .borrow_mut()
                    .push(crate::AktorError::new(alloc::format!("{phase}: {message}")));
            }
        }
    }
}

pub async fn catch<F: Future>(
    future: F,
    report: impl Fn(bool, &Box<dyn Any + Send>),
) -> Result<F::Output, Box<dyn Any + Send>> {
    let mut future = Box::pin(future);
    let result =
        poll_fn(
            |cx| match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(result) => result.map(Ok),
                Err(payload) => Poll::Ready(Err(payload)),
            },
        )
        .await;

    if let Err(payload) = &result {
        report(false, payload);
    }

    let discarded = panic::catch_unwind(AssertUnwindSafe(|| drop(future)));

    match (result, discarded) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(payload), secondary) => {
            if let Err(secondary) = secondary {
                dispose_secondary(secondary);
            }

            Err(payload)
        }
        (Ok(output), Err(payload)) => {
            report(true, &payload);

            if let Err(secondary) = panic::catch_unwind(AssertUnwindSafe(|| drop(output))) {
                dispose_secondary(secondary);
            }

            Err(payload)
        }
    }
}

pub fn discard<T>(
    values: impl IntoIterator<Item = T>,
    mut secondary: impl FnMut(&Box<dyn Any + Send>),
) -> Option<Box<dyn Any + Send>> {
    let mut primary = None;

    for value in values {
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(value))) {
            if primary.is_none() {
                primary = Some(payload);
            } else {
                secondary(&payload);
                dispose_secondary(payload);
            }
        }
    }

    primary
}

pub fn discard_hooks<S>(
    hooks: super::hooks::AktorHooks<S>,
    mut secondary: impl FnMut(&Box<dyn Any + Send>),
) -> Option<Box<dyn Any + Send>> {
    let mut primary = discard([hooks.before_each, hooks.after_each], &mut secondary);

    if let Some(payload) = discard(hooks.intervals, &mut secondary) {
        if primary.is_none() {
            primary = Some(payload);
        } else {
            secondary(&payload);
            dispose_secondary(payload);
        }
    }

    primary
}
