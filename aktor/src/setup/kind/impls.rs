use super::*;
use crate::{
    operation::Operation,
    setup::{AktorEnd, AktorIntervalLogic},
};

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
mod tokio {
    use super::*;

    #[cfg(feature = "local")]
    #[allow(non_snake_case)]
    pub fn TokioLocal(executor: &::tokio::task::LocalSet) -> TokioLocal<'_> {
        TokioLocal { executor }
    }

    impl AktorMode for TokioTask {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type End<S: 'static> = dyn crate::task::hooks::AktorTaskEnd<S> + Send;
        type Interval<S: 'static> = dyn crate::task::hooks::AktorTaskInterval<S> + Send;

        const EXECUTION: AktorExecution = AktorExecution::TokioTask;
    }

    impl AktorMode for TokioThread {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type End<S: 'static> = dyn AktorEnd<S> + Send;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S> + Send;

        const EXECUTION: AktorExecution = AktorExecution::TokioThread;
    }

    #[cfg(feature = "local")]
    impl AktorMode for TokioLocal<'_> {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type End<S: 'static> = dyn AktorEnd<S>;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::TokioLocal;
    }

    impl TokioThread {
        pub fn with_cleanup<CleanupData>(self) -> AktorLifecycle<Self, CleanupData> {
            AktorLifecycle {
                kind: self,
                data: core::marker::PhantomData,
            }
        }
    }

    impl<C: 'static> AktorMode for AktorLifecycle<TokioThread, C> {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type End<S: 'static> = dyn AktorEnd<S, C> + Send;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S> + Send;
        const EXECUTION: AktorExecution = AktorExecution::TokioThread;
    }
}

#[cfg(all(feature = "tokio", feature = "local", not(target_family = "wasm")))]
pub use tokio::TokioLocal;

#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
mod standard {
    use super::*;

    impl AktorMode for StdThread {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type End<S: 'static> = dyn AktorEnd<S> + Send;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S> + Send;

        const EXECUTION: AktorExecution = AktorExecution::StdThread;
    }
    impl StdThread {
        pub fn with_cleanup<CleanupData>(self) -> AktorLifecycle<Self, CleanupData> {
            AktorLifecycle {
                kind: self,
                data: core::marker::PhantomData,
            }
        }
    }

    impl<C: 'static> AktorMode for AktorLifecycle<StdThread, C> {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type End<S: 'static> = dyn AktorEnd<S, C> + Send;
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
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type End<S: 'static> = dyn AktorEnd<S>;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::BevyLocal;
    }

    impl AktorMode for BevyTask<'_> {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation) + Send;
        type End<S: 'static> = dyn crate::task::hooks::AktorTaskEnd<S> + Send;
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
    pub fn Custom<Clock: crate::local::clock::AktorGroupClock, Runner>(
        _clock: Clock,
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), crate::AktorSetupError>
        + 'static,
        runner: Runner,
    ) -> Custom<Clock, Runner> {
        Custom {
            spawn: alloc::rc::Rc::new(spawn),
            runner,
            clock: core::marker::PhantomData,
        }
    }

    #[allow(non_snake_case)]
    pub fn Local<Clock: crate::local::clock::AktorGroupClock>(
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), crate::AktorSetupError>
        + 'static,
    ) -> Local<Clock> {
        Local {
            spawn: alloc::rc::Rc::new(spawn),
            clock: core::marker::PhantomData,
        }
    }

    impl<Clock: crate::local::clock::AktorGroupClock, Runner> AktorMode for Custom<Clock, Runner> {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type End<S: 'static> = dyn AktorEnd<S>;
        type Interval<S: 'static> = dyn AktorIntervalLogic<S>;

        const EXECUTION: AktorExecution = AktorExecution::Custom;
    }

    impl<Clock: crate::local::clock::AktorGroupClock> AktorMode for Local<Clock> {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type End<S: 'static> = dyn AktorEnd<S>;
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
    pub fn EmbassyLocal(
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), crate::AktorSetupError>
        + 'static,
    ) -> EmbassyLocal {
        EmbassyLocal {
            spawn: alloc::rc::Rc::new(spawn),
        }
    }

    impl AktorMode for EmbassyLocal {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type End<S: 'static> = dyn AktorEnd<S>;
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
    pub fn EmbassyCrossCore(
        spawn: impl Fn(crate::message::LocalFuture<'static, ()>) -> Result<(), crate::AktorSetupError>
        + 'static,
    ) -> EmbassyCrossCore {
        EmbassyCrossCore {
            spawn: alloc::rc::Rc::new(spawn),
        }
    }

    impl AktorMode for EmbassyCrossCore {
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type End<S: 'static> = dyn AktorEnd<S>;
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
        type Each<S: 'static> = dyn FnMut(&mut S, Operation);
        type End<S: 'static> = dyn AktorEnd<S>;
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

    #[allow(non_snake_case)]
    pub fn BrowserWebWorker<S: 'static>(
        program: impl Into<alloc::string::String>,
    ) -> BrowserWebWorker<S> {
        BrowserWebWorker {
            program: program.into(),
            state: core::marker::PhantomData,
        }
    }

    impl<S> AktorMode for BrowserWebWorker<S> {
        type Each<T: 'static> = dyn FnMut(&mut T, Operation);
        type End<T: 'static> = dyn AktorEnd<T>;
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
