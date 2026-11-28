use super::*;
use crate::{operation::Operation, setup::AktorIntervalLogic};

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod tokio {
    use super::*;

    #[cfg(feature = "local")]
    #[allow(non_snake_case)]
    pub fn TokioLocal(executor: &::tokio::task::LocalSet) -> TokioLocal<'_> {
        TokioLocal { executor }
    }

    impl AktorMode for TokioTask {
        type Leave = ();
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type Interval<S: 'static> = dyn crate::task::hooks::AktorTaskInterval<S> + Send;

        const EXECUTION: AktorExecution = AktorExecution::TokioTask;
    }

    impl AktorMode for TokioThread {
        type Leave = ();
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S> + Send;

        const EXECUTION: AktorExecution = AktorExecution::TokioThread;
    }

    #[cfg(feature = "local")]
    impl AktorMode for TokioLocal<'_> {
        type Leave = core::convert::Infallible;
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::TokioLocal;
    }
}

#[cfg(all(feature = "tokio", feature = "local", not(target_family = "wasm")))]
pub use tokio::TokioLocal;

#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
mod standard {
    use super::*;

    impl AktorMode for StdThread {
        type Leave = ();
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S> + Send;

        const EXECUTION: AktorExecution = AktorExecution::StdThread;
    }
}

#[cfg(feature = "bevy")]
mod bevy {
    use super::*;

    #[allow(non_snake_case)]
    pub fn BevyTask(executor: &bevy_tasks::TaskPool) -> BevyTask<'_> {
        BevyTask { executor }
    }

    #[allow(non_snake_case)]
    pub fn BevyLocal(executor: &bevy_tasks::TaskPool) -> BevyLocal<'_> {
        BevyLocal { executor }
    }

    impl AktorMode for BevyLocal<'_> {
        type Leave = core::convert::Infallible;
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::BevyLocal;
    }

    impl AktorMode for BevyTask<'_> {
        type Leave = ();
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type Interval<S: 'static> = dyn crate::task::hooks::AktorTaskInterval<S> + Send;

        const EXECUTION: AktorExecution = AktorExecution::BevyTask;
    }
}

#[cfg(feature = "bevy")]
pub use bevy::*;

#[cfg(feature = "local")]
mod local {
    use super::*;

    #[allow(non_snake_case)]
    pub fn Custom<
        Clock: crate::local::clock::AktorGroupClock,
        Runner,
        E,
        X: Into<(E, alloc::string::String)>,
    >(
        _clock: Clock,
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), X> + 'static,
        runner: Runner,
    ) -> Custom<Clock, Runner> {
        Custom {
            spawn: alloc::rc::Rc::new(move |future| match spawn(future) {
                Ok(()) => Ok(()),
                Err(error) => Err(error.into().1),
            }),
            runner,
            clock: core::marker::PhantomData,
        }
    }

    #[allow(non_snake_case)]
    pub fn Local<
        Clock: crate::local::clock::AktorGroupClock,
        E,
        X: Into<(E, alloc::string::String)>,
    >(
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), X> + 'static,
    ) -> Local<Clock> {
        Local {
            spawn: alloc::rc::Rc::new(move |future| match spawn(future) {
                Ok(()) => Ok(()),
                Err(error) => Err(error.into().1),
            }),
            clock: core::marker::PhantomData,
        }
    }

    impl<Clock: crate::local::clock::AktorGroupClock, Runner> AktorMode for Custom<Clock, Runner> {
        type Leave = core::convert::Infallible;
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::Custom;
    }

    impl<Clock: crate::local::clock::AktorGroupClock> AktorMode for Local<Clock> {
        type Leave = core::convert::Infallible;
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::Local;
    }
}

#[cfg(feature = "local")]
pub use local::*;

#[cfg(feature = "embassy")]
mod embassy {
    use super::*;

    #[allow(non_snake_case)]
    pub fn EmbassyLocal<E, X: Into<(E, alloc::string::String)>>(
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), X> + 'static,
    ) -> EmbassyLocal {
        EmbassyLocal {
            spawn: alloc::rc::Rc::new(move |future| match spawn(future) {
                Ok(()) => Ok(()),
                Err(error) => Err(error.into().1),
            }),
        }
    }

    impl AktorMode for EmbassyLocal {
        type Leave = core::convert::Infallible;
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::EmbassyLocal;
    }
}

#[cfg(feature = "embassy")]
pub use embassy::*;

#[cfg(feature = "embassy_cross_core")]
mod cross_core {
    use super::*;

    #[allow(non_snake_case)]
    pub fn EmbassyCrossCore<E, X: Into<(E, alloc::string::String)>>(
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), X> + 'static,
    ) -> EmbassyCrossCore {
        EmbassyCrossCore {
            spawn: alloc::rc::Rc::new(move |future| match spawn(future) {
                Ok(()) => Ok(()),
                Err(error) => Err(error.into().1),
            }),
        }
    }

    impl AktorMode for EmbassyCrossCore {
        type Leave = core::convert::Infallible;
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;
        const EXECUTION: AktorExecution = AktorExecution::EmbassyCrossCore;
    }
}

#[cfg(feature = "embassy_cross_core")]
pub use cross_core::*;

#[cfg(all(
    feature = "browser_local",
    target_family = "wasm",
    target_os = "unknown"
))]
mod browser {
    use super::*;

    impl AktorMode for BrowserLocal {
        type Leave = core::convert::Infallible;
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::BrowserLocal;
    }
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
mod worker {
    use super::*;
    use crate::AktorWorkerContract;

    #[allow(non_snake_case, clippy::type_complexity)]
    pub fn BrowserWebWorker<C: AktorWorkerContract>(
        program: impl Into<alloc::string::String>,
    ) -> BrowserWebWorker<C> {
        BrowserWebWorker {
            program: program.into(),
            state: PhantomData,
        }
    }

    impl<C: AktorWorkerContract> AktorMode for BrowserWebWorker<C> {
        type Leave = ();
        type Each<T: 'static> = dyn FnMut(&mut T, Operation);
        type Interval<T: 'static> = dyn AktorIntervalLogic<T>;

        const EXECUTION: AktorExecution = AktorExecution::BrowserWebWorker;
    }
}

#[cfg(all(
    feature = "wasm_browser_workers",
    target_family = "wasm",
    target_os = "unknown"
))]
pub use worker::*;

#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_os = "unknown",
    target_feature = "atomics"
))]
#[allow(non_snake_case)]
pub fn BrowserThread(program: impl Into<alloc::string::String>) -> BrowserThread {
    BrowserThread {
        program: program.into(),
    }
}

#[cfg(all(
    feature = "wasm_threads",
    target_family = "wasm",
    target_os = "unknown",
    target_feature = "atomics"
))]
impl AktorMode for BrowserThread {
    type Leave = ();
    type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
    type Interval<S: 'static> = dyn AktorIntervalLogic<S> + Send;

    const EXECUTION: AktorExecution = AktorExecution::BrowserThread;
}
