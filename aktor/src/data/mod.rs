//! Compact worker data, independent of the type's ordinary Serde representation.
//!
//! Derive [`AktorData`] on transported structs and enums. `#[aktor(skip)]` leaves
//! a field local and restores its default when receiving the value.

mod decode;
mod encode;
mod impls;
mod types;

pub use decode::decode;
pub use encode::encode;
#[doc(hidden)]
pub use serde as __serde;
pub use types::{AktorData, Owned, Ref};

const MAX_DEPTH: usize = 128;

fn value_budget(bytes: usize) -> usize {
    bytes.saturating_mul(128).saturating_add(1024)
}
