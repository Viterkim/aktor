use alloc::string::String;
use core::{marker::PhantomData, panic::Location};

// Give the body its declared type, including coercions and aliases. A pair also accepts
// a convertible error. Prefer the identity on the way out so a bare Ok can infer its error.
#[doc(hidden)]
pub trait CallResult<I, O>: Sized {
    fn body(self, value: I) -> I {
        value
    }

    fn finish(self, value: I) -> O;
}
impl<T> CallResult<T, T> for &PhantomData<fn(T) -> T> {
    fn finish(self, value: T) -> T {
        value
    }
}
impl<T, E, X: Into<(E, String)>> CallResult<Result<T, X>, Result<T, (E, String)>>
    for &&PhantomData<fn(Result<T, X>) -> Result<T, (E, String)>>
{
    fn finish(self, value: Result<T, X>) -> Result<T, (E, String)> {
        match value {
            Ok(value) => Ok(value),
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Operation {
    pub name: &'static str,
    pub caller: &'static Location<'static>,
}

#[cfg(any(
    feature = "local",
    feature = "tokio",
    all(feature = "wasm_browser_workers", target_family = "wasm"),
    all(feature = "std_thread", not(target_family = "wasm"))
))]
impl Operation {
    pub(crate) const INTERVAL: Self = Self {
        name: "interval",
        caller: Location::caller(),
    };

    #[cfg(any(
        feature = "std",
        all(feature = "wasm_browser_workers", target_family = "wasm")
    ))]
    pub(crate) fn phase(self) -> crate::AktorPhase {
        use alloc::string::ToString;
        if self.caller == Self::INTERVAL.caller {
            return crate::AktorPhase::Interval;
        }
        crate::AktorPhase::Call {
            name: self.name.into(),
            caller: Some(self.caller.to_string().into()),
        }
    }
}

pub mod hooks;
