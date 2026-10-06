pub use crate::local::{
    ActorArgs, Completion, LatestResults, LatestSender, Owner, OwnerError, ShutdownReport,
};
use crate::{local, message::ActorError};

pub type AktorGroup = local::AktorGroup<local::clock::Embassy>;
pub type GroupCompletion = local::GroupCompletion<local::clock::Embassy>;
pub type KillSwitch = local::KillSwitch<local::clock::Embassy>;

pub type Handle<S, const N: usize, E = core::convert::Infallible, Role = ()> =
    local::Handle<S, N, E, Role, local::clock::Embassy>;
pub type WeakHandle<S, const N: usize, E = core::convert::Infallible, Role = ()> =
    local::WeakHandle<S, N, E, Role, local::clock::Embassy>;
pub type Request<'a, S, const N: usize, E, O, Role = ()> =
    local::Request<'a, S, N, E, O, Role, local::clock::Embassy>;
pub type Reply<O> = local::Reply<O, local::clock::Embassy>;
pub type Channel<S, const N: usize, E = core::convert::Infallible> =
    local::Channel<S, N, E, local::clock::Embassy>;

/// Capacity bounds queued work. One operation can also be running.
pub fn channel<S, const N: usize, E>() -> Result<Channel<S, N, E>, ActorError> {
    local::channel_with_clock::<S, N, E, local::clock::Embassy>(N)
}
