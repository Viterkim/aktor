use crate::operation::Operation;
use core::ops::AsyncFnOnce;

/// Only marked functions get one, so a missing impl means a missing `#[aktor(latest)]`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`latest` needs `#[aktor(latest)]` on this function",
    label = "this function wasn't marked `latest`",
    note = "in a generic helper that forwards a `Call`, add `L: aktor::latest::Factory<T, I>` to the helper instead"
)]
pub trait Factory<T, I> {
    type Sender: SendLatest<I>;
    type Results;

    fn create(self, target: T, operation: Operation) -> (Self::Sender, Self::Results);
}

/// The factory of a function without `latest`. It never implements `Factory`, so `.latest()` on
/// it fails with `Factory`'s message.
#[doc(hidden)]
pub struct AddAktorLatest;

/// Never implemented. Stands in for `latest` on a function that wasn't marked, so calling it says
/// what's missing.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`latest` needs `#[aktor(latest)]` on this function",
    label = "this function wasn't marked `latest`"
)]
pub trait Marked {}

crate::__aktor_body! {
    body {
        /// What `my_fn::Sender` holds when you don't name it: the session of this build's normal actor
        /// handle, so storing a native actor's latest sender is just `my_fn::Sender`.
        #[doc(hidden)]
        #[cfg(any(
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm")),
            feature = "bevy"
        ))]
        pub type DefaultSender<I> = crate::queue::latest::LatestSender<I>;
        #[doc(hidden)]
        #[cfg(all(
            feature = "local",
            not(any(
                feature = "tokio",
                all(feature = "std_thread", not(target_family = "wasm")),
                feature = "bevy"
            ))
        ))]
        pub type DefaultSender<I> = crate::local::LatestSender<I>;
        #[doc(hidden)]
        #[cfg(not(any(
            feature = "local",
            feature = "tokio",
            all(feature = "std_thread", not(target_family = "wasm")),
            feature = "bevy"
        )))]
        pub type DefaultSender<I> = core::marker::PhantomData<I>;
    }
    page {
        #[doc(hidden)]
        #[cfg(target_os = "unknown")]
        pub type DefaultSender<I> = crate::worker::LatestSender<I>;

        #[doc(hidden)]
        #[cfg(not(target_os = "unknown"))]
        pub type DefaultSender<I> = core::marker::PhantomData<I>;
    }
}

/// Update one ongoing operation.
pub trait SendLatest<I> {
    fn send(&self, input: I);
}

#[doc(hidden)]
pub trait Session<S, I, O, Role = (), Codec = crate::dispatch::DataCodec> {
    type Sender: SendLatest<I>;
    type Results;

    fn session<F>(self, operation: Operation, function: F) -> (Self::Sender, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static;
}

#[doc(hidden)]
pub trait TypedSession<S, I, O, Lease, Role = (), Codec = crate::dispatch::DataCodec> {
    type Sender<Fut>;
    type Results;

    fn session<F, Fut>(
        self,
        operation: Operation,
        function: F,
        factory: fn(Lease, I) -> Fut,
    ) -> (Self::Sender<Fut>, Self::Results)
    where
        F: for<'s> AsyncFnOnce(&'s mut S, I) -> O + Clone + Send + 'static,
        Fut: core::future::Future<Output = (Lease, O)>;
}

#[doc(hidden)]
pub trait WorkerSession<S, I, O, Role, C> {
    type Sender: SendLatest<I>;
    type Results;

    fn open_session(self, operation: Operation) -> (Self::Sender, Self::Results);
}
