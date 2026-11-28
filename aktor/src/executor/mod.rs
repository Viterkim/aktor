#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
pub mod shared;

mod impls;

pub use impls::block_on;
#[cfg(not(target_family = "wasm"))]
pub use impls::sleep_until;
#[cfg(test)]
pub use impls::{FAIL_SPAWN, FAIL_SPAWN_AFTER, check_spawn};

use er::*;

#[derive(Er)]
pub enum DriverError {
    #[er(format = "could not build the actor runtime")]
    Runtime,
    #[er(format = "the Tokio driver requires the tokio feature")]
    Disabled,
}

pub enum Driver {
    #[cfg(feature = "tokio")]
    Tokio(tokio::runtime::Runtime),
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    Std,
}

#[derive(Clone)]
pub enum Spawner {
    #[cfg(feature = "tokio")]
    Tokio(tokio::runtime::Handle),
    #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
    Std,
}
