use super::{AktorInitOutcome, AktorStartCause, AktorStartError};
use crate::{AktorError, AktorGroupError};
use alloc::string::String;
use core::{convert::Infallible, error::Error};

/// Return an actor's state from an initializer that can't fail.
pub const fn aktor_ready<S>(state: S) -> Result<S, Infallible> {
    Ok(state)
}

/// Which actor in a setup tuple returned its initializer error, in the same order as the handles.
#[derive(Debug)]
pub enum AktorBranch<
    A,
    B,
    C = Infallible,
    D = Infallible,
    E = Infallible,
    F = Infallible,
    G = Infallible,
    H = Infallible,
> {
    First(A),
    Second(B),
    Third(C),
    Fourth(D),
    Fifth(E),
    Sixth(F),
    Seventh(G),
    Eighth(H),
}

#[doc(hidden)]
pub struct Plain;
#[doc(hidden)]
pub struct Text;
#[doc(hidden)]
pub struct Pair;
#[doc(hidden)]
pub struct Tree;

#[doc(hidden)]
pub trait InitError<Mode> {
    type Error;
    fn split(self) -> (Self::Error, String);

    fn retain_with_report(self) -> (impl Sized, String)
    where
        Self: Sized,
    {
        self.split()
    }
}

impl<E: Error + 'static> InitError<Plain> for E {
    type Error = E;

    fn split(self) -> (E, String) {
        let report = super::diagnostics(&self);
        (self, report)
    }
}
impl InitError<Text> for String {
    type Error = ();

    fn split(self) -> ((), String) {
        ((), self)
    }
}
impl<E> InitError<Pair> for (E, String) {
    type Error = E;

    fn split(self) -> Self {
        self
    }
}
impl<E: Error + 'static> InitError<Tree> for er::ErTree<E> {
    type Error = E;

    fn split(self) -> (E, String) {
        let report = crate::report::tree_report(&self);
        (self.top, report)
    }

    fn retain_with_report(self) -> (impl Sized, String) {
        let report = crate::report::tree_report(&self);
        (self, report)
    }
}

impl<E> AktorStartError<E> {
    pub(crate) fn not_run(cause: AktorStartCause) -> Self {
        Self {
            init: AktorInitOutcome::NotRun,
            cause,
            report: None,
        }
    }

    pub(crate) fn map_init<T>(self, f: impl FnOnce(E) -> T) -> AktorStartError<T> {
        AktorStartError {
            init: match self.init {
                AktorInitOutcome::NotRun => AktorInitOutcome::NotRun,
                AktorInitOutcome::Failed(error) => AktorInitOutcome::Failed(f(error)),
                AktorInitOutcome::Unconfirmed => AktorInitOutcome::Unconfirmed,
            },
            cause: self.cause,
            report: self.report,
        }
    }
}

impl<E> From<AktorStartCause> for AktorStartError<E> {
    fn from(cause: AktorStartCause) -> Self {
        Self {
            init: AktorInitOutcome::Unconfirmed,
            cause,
            report: None,
        }
    }
}
impl<E> From<AktorError> for AktorStartError<E> {
    fn from(error: AktorError) -> Self {
        AktorStartCause::Setup(error).into()
    }
}
impl<E> From<AktorGroupError> for AktorStartError<E> {
    fn from(error: AktorGroupError) -> Self {
        AktorStartCause::Group(error).into()
    }
}
