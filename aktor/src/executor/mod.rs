mod impls;
#[cfg(test)]
pub use impls::FAIL_SPAWN;
pub use impls::block_on;
#[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
pub use impls::sleep_until;

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
