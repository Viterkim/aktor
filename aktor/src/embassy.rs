use crate::local;
#[doc(hidden)]
pub use crate::local::{AktorArgs, Completion, Owner};
pub use crate::local::{AktorShutdownReport, LatestResults, LatestSender, OwnerError};

pub type AktorGroup = local::AktorGroup<local::clock::Embassy>;

/// Return a sensor or other Embassy actor setup from a helper.
pub trait AktorDefinition<Handle, E = core::convert::Infallible>:
    crate::setup::AktorStart<Group = AktorGroup, Handles = Handle, Error = E>
{
}
impl<T, Handle, E> AktorDefinition<Handle, E> for T where
    T: crate::setup::AktorStart<Group = AktorGroup, Handles = Handle, Error = E>
{
}

pub type AktorGroupCompletion = local::AktorGroupCompletion<local::clock::Embassy>;
pub type AktorKillSwitch = local::AktorKillSwitch<local::clock::Embassy>;

pub type Handle<S, Role = ()> = local::Handle<S, Role, local::clock::Embassy>;
pub type WeakHandle<S, Role = ()> = local::WeakHandle<S, Role, local::clock::Embassy>;
pub type Request<'a, S, O, Role = ()> = local::Request<'a, S, O, Role, local::clock::Embassy>;
pub type Reply<O> = local::Reply<O, local::clock::Embassy>;
#[doc(hidden)]
pub type Channel<S, E = core::convert::Infallible> = local::Channel<S, E, local::clock::Embassy>;

/// Capacity bounds queued work. One operation can also be running.
#[doc(hidden)]
pub fn channel<S, E>(capacity: core::num::NonZeroUsize) -> Channel<S, E> {
    local::channel_with_clock::<S, E, local::clock::Embassy>(capacity)
}
