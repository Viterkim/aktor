use alloc::vec::Vec;
use serde::{Deserializer, Serializer};

/// A compact transport representation which leaves ordinary Serde unchanged.
///
/// `#[derive(AktorData)]` uses Rust fields and variants. `#[aktor(skip)]` omits
/// a field and restores `Default::default()` when decoding it.
pub trait AktorData: Sized {
    #[doc(hidden)]
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error>;

    #[doc(hidden)]
    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error>;

    #[doc(hidden)]
    fn serialize_slice<S: Serializer>(values: &[Self], serializer: S) -> Result<S::Ok, S::Error> {
        super::impls::serialize_slice(values, serializer)
    }

    #[doc(hidden)]
    fn deserialize_vec<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Self>, D::Error> {
        super::impls::deserialize_vec(deserializer)
    }

    #[doc(hidden)]
    fn serialize_array<S: Serializer, const N: usize>(
        values: &[Self; N],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        super::impls::serialize_array(values, serializer)
    }

    #[doc(hidden)]
    fn deserialize_array<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<[Self; N], D::Error> {
        super::impls::deserialize_array(deserializer)
    }
}

#[doc(hidden)]
pub struct Ref<'a, T: ?Sized>(pub &'a T);

#[doc(hidden)]
pub struct Owned<T>(pub T);
