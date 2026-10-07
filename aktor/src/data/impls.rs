use super::{AktorData, Owned, Ref};
use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet, VecDeque},
    string::String,
    vec::Vec,
};
use core::{fmt, marker::PhantomData};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
    ser::{SerializeSeq, SerializeTuple},
};

impl<T: AktorData> Serialize for Ref<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize_data(serializer)
    }
}

impl<'de, T: AktorData> Deserialize<'de> for Owned<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize_data(deserializer).map(Self)
    }
}

pub fn serialize_slice<T: AktorData, S: Serializer>(
    values: &[T],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut sequence = serializer.serialize_seq(Some(values.len()))?;

    for value in values {
        sequence.serialize_element(&Ref(value))?;
    }

    sequence.end()
}

pub fn deserialize_vec<'de, T: AktorData, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    Vec::<Owned<T>>::deserialize(deserializer)
        .map(|values| values.into_iter().map(|value| value.0).collect())
}

macro_rules! scalar {
    ($($ty:ty),* $(,)?) => { $(
        impl AktorData for $ty {
            fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.serialize(serializer)
            }

            fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Self::deserialize(deserializer)
            }
        }
    )* };
}
scalar!(
    (),
    bool,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    u16,
    u32,
    u64,
    u128,
    usize,
    f32,
    f64,
    char,
    String
);

impl AktorData for u8 {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self)
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::deserialize(deserializer)
    }

    fn serialize_slice<S: Serializer>(values: &[Self], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(values)
    }

    fn deserialize_vec<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Self>, D::Error> {
        struct Bytes;
        impl<'de> Visitor<'de> for Bytes {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("bytes")
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
                Ok(bytes.to_vec())
            }
        }

        deserializer.deserialize_bytes(Bytes)
    }
}

impl<T: AktorData> AktorData for Vec<T> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        T::serialize_slice(self, serializer)
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize_vec(deserializer)
    }
}

impl<T: AktorData> AktorData for Option<T> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_ref().map(Ref).serialize(serializer)
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<Owned<T>>::deserialize(deserializer).map(|value| value.map(|value| value.0))
    }
}

impl<T: AktorData, E: AktorData> AktorData for Result<T, E> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_ref().map(Ref).map_err(Ref).serialize(serializer)
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Result::<Owned<T>, Owned<E>>::deserialize(deserializer)
            .map(|value| value.map(|value| value.0).map_err(|value| value.0))
    }
}

impl<T: AktorData> AktorData for Box<T> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_ref().serialize_data(serializer)
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize_data(deserializer).map(Box::new)
    }
}
impl<T: AktorData, const N: usize> AktorData for [T; N] {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(N)?;

        for value in self {
            tuple.serialize_element(&Ref(value))?;
        }

        tuple.end()
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Array<T, const N: usize>(PhantomData<T>);
        impl<'de, T: AktorData, const N: usize> Visitor<'de> for Array<T, N> {
            type Value = [T; N];

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "an array of {N} elements")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();

                for index in 0..N {
                    let value = sequence
                        .next_element::<Owned<T>>()?
                        .ok_or_else(|| serde::de::Error::invalid_length(index, &self))?;

                    values.push(value.0);
                }

                values
                    .try_into()
                    .map_err(|_| serde::de::Error::custom("invalid array length"))
            }
        }

        deserializer.deserialize_tuple(N, Array::<T, N>(PhantomData))
    }
}

macro_rules! tuple {
    ($(($($ty:ident:$index:tt),+)),* $(,)?) => { $(
        impl<$($ty: AktorData),+> AktorData for ($($ty,)+) {
            fn serialize_data<Se: Serializer>(&self, serializer: Se) -> Result<Se::Ok, Se::Error> {
                ($(Ref(&self.$index),)+).serialize(serializer)
            }

            fn deserialize_data<'de, De: Deserializer<'de>>(deserializer: De) -> Result<Self, De::Error> {
                <($(Owned<$ty>,)+)>::deserialize(deserializer).map(|values| ($(values.$index.0,)+))
            }
        }
    )* };
}
tuple!(
    (A:0),
    (A:0,B:1),
    (A:0,B:1,C:2),
    (A:0,B:1,C:2,D:3),
    (A:0,B:1,C:2,D:3,E:4),
    (A:0,B:1,C:2,D:3,E:4,F:5),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11)
);

impl<T: AktorData> AktorData for VecDeque<T> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;

        for value in self {
            sequence.serialize_element(&Ref(value))?;
        }

        sequence.end()
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<T>::deserialize_data(deserializer).map(VecDeque::from)
    }
}

impl<T: AktorData + Ord> AktorData for BTreeSet<T> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;

        for value in self {
            sequence.serialize_element(&Ref(value))?;
        }

        sequence.end()
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<T>::deserialize_data(deserializer).map(|values| values.into_iter().collect())
    }
}

impl<K: AktorData + Ord, V: AktorData> AktorData for BTreeMap<K, V> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.len()))?;

        for (key, value) in self {
            map.serialize_entry(&Ref(key), &Ref(value))?;
        }

        map.end()
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Entries<K, V>(PhantomData<(K, V)>);
        impl<'de, K: AktorData + Ord, V: AktorData> Visitor<'de> for Entries<K, V> {
            type Value = BTreeMap<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = BTreeMap::new();

                while let Some((key, value)) = map.next_entry::<Owned<K>, Owned<V>>()? {
                    values.insert(key.0, value.0);
                }

                Ok(values)
            }
        }

        deserializer.deserialize_map(Entries(PhantomData))
    }
}

impl<T: AktorData> AktorData for Box<[T]> {
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        T::serialize_slice(self, serializer)
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize_vec(deserializer).map(Vec::into_boxed_slice)
    }
}

impl<K, V, H> AktorData for std::collections::HashMap<K, V, H>
where
    K: AktorData + Eq + core::hash::Hash,
    V: AktorData,
    H: core::hash::BuildHasher + Default,
{
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.len()))?;

        for (key, value) in self {
            map.serialize_entry(&Ref(key), &Ref(value))?;
        }

        map.end()
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Entries<K, V, H>(PhantomData<(K, V, H)>);
        impl<'de, K, V, H> Visitor<'de> for Entries<K, V, H>
        where
            K: AktorData + Eq + core::hash::Hash,
            V: AktorData,
            H: core::hash::BuildHasher + Default,
        {
            type Value = std::collections::HashMap<K, V, H>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = std::collections::HashMap::with_hasher(H::default());

                while let Some((key, value)) = map.next_entry::<Owned<K>, Owned<V>>()? {
                    values.insert(key.0, value.0);
                }

                Ok(values)
            }
        }

        deserializer.deserialize_map(Entries(PhantomData))
    }
}

impl<T, H> AktorData for std::collections::HashSet<T, H>
where
    T: AktorData + Eq + core::hash::Hash,
    H: core::hash::BuildHasher + Default,
{
    fn serialize_data<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;

        for value in self {
            sequence.serialize_element(&Ref(value))?;
        }

        sequence.end()
    }

    fn deserialize_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let values = T::deserialize_vec(deserializer)?;
        let mut set =
            std::collections::HashSet::with_capacity_and_hasher(values.len(), H::default());

        set.extend(values);

        Ok(set)
    }
}
