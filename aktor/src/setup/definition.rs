use super::AktorStart;
use crate::AktorGroup;
use core::convert::Infallible;

/// Return a native actor setup from a helper as `impl AktorDefinition<Handle, InitErr>`.
#[cfg(not(target_family = "wasm"))]
pub trait AktorDefinition<Handle, E = Infallible>:
    AktorStart<Group = AktorGroup, Handles = Handle, Error = E, Startup: Send + 'static> + Send
{
}
#[cfg(not(target_family = "wasm"))]
impl<T, Handle, E> AktorDefinition<Handle, E> for T where
    T: AktorStart<Group = AktorGroup, Handles = Handle, Error = E, Startup: Send + 'static> + Send
{
}

/// Return a setup that runs on a local executor.
#[cfg(feature = "local")]
pub trait LocalDefinition<Handle, E = Infallible, Group = AktorGroup>:
    AktorStart<Group = Group, Handles = Handle, Error = E>
{
}
#[cfg(feature = "local")]
impl<T, Handle, E, Group> LocalDefinition<Handle, E, Group> for T where
    T: AktorStart<Group = Group, Handles = Handle, Error = E>
{
}

#[cfg(target_family = "wasm")]
pub use LocalDefinition as AktorDefinition;
