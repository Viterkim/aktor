#[doc(hidden)]
pub use crate::local::{AktorArgs, Completion, Owner};
pub use crate::local::{AktorShutdownReport, LatestResults, LatestSender, OwnerError};
use crate::{local, message::AktorStateError};

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

pub type Handle<S, const N: usize, E = core::convert::Infallible, Role = ()> =
    local::Handle<S, N, E, Role, local::clock::Embassy>;
pub type WeakHandle<S, const N: usize, E = core::convert::Infallible, Role = ()> =
    local::WeakHandle<S, N, E, Role, local::clock::Embassy>;
pub type Request<'a, S, const N: usize, E, O, Role = ()> =
    local::Request<'a, S, N, E, O, Role, local::clock::Embassy>;
pub type Reply<O> = local::Reply<O, local::clock::Embassy>;
#[doc(hidden)]
pub type Channel<S, const N: usize, E = core::convert::Infallible> =
    local::Channel<S, N, E, local::clock::Embassy>;

/// Capacity bounds queued work. One operation can also be running.
#[doc(hidden)]
pub fn channel<S, const N: usize, E>() -> Result<Channel<S, N, E>, AktorStateError> {
    local::channel_with_clock::<S, N, E, local::clock::Embassy>(N)
}
