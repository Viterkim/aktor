#![cfg(all(feature = "macros", feature = "wasm_browser_workers"))]
use aktor::{AktorData, data, worker};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

#[derive(AktorData, Debug, PartialEq)]
struct Row {
    id: u64,
    name: String,
    bytes: Vec<u8>,
}
#[derive(AktorData, Debug, PartialEq)]
struct Nested<r#T> {
    rows: Vec<Result<T, Choice>>,
    pair: (u32, Option<String>),
}
#[derive(AktorData, Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
enum Choice {
    Empty,
    Text(String),
    Count(u64),
    Named { value: u8 },
}
#[derive(AktorData, Debug, PartialEq)]
struct Pair(u64, String);
#[derive(AktorData, Debug, PartialEq)]
struct Unit;
#[derive(AktorData, Debug, PartialEq)]
struct Array<const N: usize> {
    values: [u64; N],
}
#[derive(AktorData, Debug, PartialEq)]
enum Tree<r#T> {
    Leaf(r#T),
    Branch(Vec<Tree<r#T>>),
}
#[derive(AktorData)]
struct Node {
    next: Option<Box<Node>>,
}
#[derive(AktorData, Debug, PartialEq)]
struct SelfNode {
    value: u32,
    next: Option<Box<Self>>,
    bytes: [u8; Self::WIDTH],
}
impl SelfNode {
    const WIDTH: usize = 3;
}
#[derive(AktorData, Debug, PartialEq)]
enum SelfTree<T> {
    Leaf(T),
    Branch(Vec<Self>),
}
trait Value {
    type Item;
}
#[derive(AktorData, Debug, PartialEq)]
struct Qualified<T: AktorData>
where
    Self: Value<Item = T>,
{
    value: <Self as Value>::Item,
}
impl<T: AktorData> Value for Qualified<T> {
    type Item = T;
}
#[derive(AktorData, Serialize, Deserialize)]
struct Custom {
    value: u64,
}

#[derive(Default)]
struct Cache {
    hits: usize,
}
#[derive(AktorData)]
struct Cached<T> {
    value: u64,
    #[aktor(skip)]
    cache: T,
}
#[derive(AktorData)]
struct Local<T> {
    #[aktor(skip)]
    cache: T,
}
#[derive(AktorData, Serialize, Deserialize)]
struct Visible {
    value: u64,
    #[aktor(skip)]
    note: String,
}
#[derive(AktorData, Serialize, Deserialize)]
struct Hidden {
    value: u64,
    #[aktor(skip)]
    #[serde(skip)]
    cache: Cache,
}
#[derive(AktorData)]
enum WithCache<T> {
    Named {
        value: u64,
        #[aktor(skip)]
        cache: T,
    },
    Tuple(u64, #[aktor(skip)] T),
    Empty,
}
#[derive(AktorData)]
struct BorrowedCache<'de> {
    value: u64,
    #[aktor(skip)]
    cache: &'de str,
}
#[derive(AktorData)]
struct OwnSerde {
    value: u64,
}
impl Serialize for OwnSerde {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("user-format")
    }
}
impl<'de> Deserialize<'de> for OwnSerde {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let _ = String::deserialize(deserializer)?;
        Ok(Self { value: 0 })
    }
}

fn roundtrip<T: AktorData + PartialEq + std::fmt::Debug>(value: T) {
    let bytes = data::encode(&value).unwrap();
    assert_eq!(data::decode::<T>(&bytes).unwrap(), value);
    for end in 0..bytes.len() {
        assert!(data::decode::<T>(&bytes[..end]).is_err());
    }

    let mut extra = bytes;
    extra.push(0);
    assert!(data::decode::<T>(&extra).is_err());
}

#[test]
fn portable_values() {
    roundtrip(Nested {
        rows: vec![
            Ok(Row {
                id: 7,
                name: "row".into(),
                bytes: (0..=255).collect(),
            }),
            Err(Choice::Named { value: 19 }),
        ],
        pair: (23, Some("value".into())),
    });

    roundtrip(Choice::Empty);
    roundtrip(Choice::Text("a".into()));
    roundtrip(Choice::Count(u64::MAX));

    roundtrip(Pair(17, "pair".into()));
    roundtrip(Unit);
    roundtrip(Array { values: [1, 2, 3] });

    roundtrip(Tree::Branch(vec![
        Tree::Leaf(7u64),
        Tree::Branch(vec![Tree::Leaf(19)]),
    ]));

    roundtrip(SelfNode {
        value: 7,
        bytes: [1, 2, 3],
        next: Some(Box::new(SelfNode {
            value: 19,
            next: None,
            bytes: [4, 5, 6],
        })),
    });
    let tree = SelfTree::Branch(vec![
        SelfTree::Leaf(7u64),
        SelfTree::Branch(vec![SelfTree::Leaf(19)]),
    ]);
    assert_eq!(
        data::encode(&tree).unwrap(),
        data::encode(&Tree::Branch(vec![
            Tree::Leaf(7u64),
            Tree::Branch(vec![Tree::Leaf(19)]),
        ]))
        .unwrap()
    );
    roundtrip(tree);
    roundtrip(Qualified { value: 23u32 });

    roundtrip(BTreeMap::from([(
        7u64,
        BTreeSet::from(["value".to_owned()]),
    )]));
    roundtrip(VecDeque::from([1u32, 2, 3]));
    roundtrip(HashMap::from([("key".to_owned(), 7u64)]));
    roundtrip(HashSet::from(["key".to_owned()]));
    roundtrip(vec![1u8, 2, 3].into_boxed_slice());

    roundtrip((
        i128::MIN,
        u128::MAX,
        'ø',
        -7isize,
        usize::MAX,
        3.25f64,
        true,
    ));
    roundtrip((
        1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8, 10u8, 11u8, 12u8,
    ));

    let bytes = vec![37u8; 1024 * 1024];

    let encoded = data::encode(&bytes).unwrap();
    assert!(encoded.len() < bytes.len() + 8);
    assert_eq!(data::decode::<Vec<u8>>(&encoded).unwrap(), bytes);
}

#[test]
fn independent_representation() {
    let custom = OwnSerde { value: 99 };
    assert_eq!(
        data::decode::<OwnSerde>(&data::encode(&custom).unwrap())
            .unwrap()
            .value,
        99
    );
    assert_eq!(
        worker::decode::<String>(&worker::encode(&custom).unwrap()).unwrap(),
        "user-format"
    );

    let visible = Visible {
        value: 7,
        note: "ordinary Serde keeps it".into(),
    };
    assert_eq!(
        worker::decode::<Visible>(&worker::encode(&visible).unwrap())
            .unwrap()
            .note,
        visible.note
    );
    assert!(
        data::decode::<Visible>(&data::encode(&visible).unwrap())
            .unwrap()
            .note
            .is_empty()
    );

    let borrowed = BorrowedCache {
        value: 31,
        cache: "local",
    };

    let restored: BorrowedCache<'_> = data::decode(&data::encode(&borrowed).unwrap()).unwrap();
    assert_eq!(restored.value, 31);
    assert!(restored.cache.is_empty());

    let cached = Cached {
        value: 19,
        cache: Cache { hits: 88 },
    };

    let restored = data::decode::<Cached<Cache>>(&data::encode(&cached).unwrap()).unwrap();
    assert_eq!((restored.value, restored.cache.hits), (19, 0));

    let only = Local {
        cache: Cache { hits: 55 },
    };

    let bytes = data::encode(&only).unwrap();
    assert!(bytes.is_empty());
    assert_eq!(data::decode::<Local<Cache>>(&bytes).unwrap().cache.hits, 0);

    let hidden = Hidden {
        value: 21,
        cache: Cache { hits: 99 },
    };
    assert_eq!(
        worker::decode::<Hidden>(&worker::encode(&hidden).unwrap())
            .unwrap()
            .cache
            .hits,
        0
    );

    for value in [
        WithCache::Named {
            value: 7,
            cache: Cache { hits: 11 },
        },
        WithCache::Tuple(19, Cache { hits: 22 }),
        WithCache::Empty,
    ] {
        match data::decode::<WithCache<Cache>>(&data::encode(&value).unwrap()).unwrap() {
            WithCache::Named { value, cache } | WithCache::Tuple(value, cache) => {
                assert!(value > 0);
                assert_eq!(cache.hits, 0);
            }
            WithCache::Empty => {}
        }
    }

    let custom = Custom { value: 5 };
    assert_eq!(
        data::encode(&custom).unwrap(),
        postcard::to_allocvec(&custom).unwrap()
    );
}

#[test]
fn bounded_decode() {
    let mut node = Node { next: None };
    for _ in 0..140 {
        node = Node {
            next: Some(Box::new(node)),
        };
    }

    let bytes = postcard::to_allocvec(&data::Ref(&node)).unwrap();
    assert!(data::encode(&node).is_err());

    let error = data::decode::<Node>(&bytes).err().unwrap();
    assert!(error.to_string().contains("Codec"));

    for count in [0, 1, 100, 500] {
        roundtrip(vec![(); count]);
    }
    for count in [0, 1, 20, 50] {
        let values = (0..count).map(|_| (Unit, ((), Unit))).collect::<Vec<_>>();
        roundtrip(values);
    }

    let values = vec![(); 1_000];
    let bytes = postcard::to_allocvec(&data::Ref(&values)).unwrap();
    assert!(data::decode::<Vec<()>>(&bytes).is_err());
    assert!(data::encode(&values).is_err());

    let values = (0..1_000).map(|_| (Unit, ((), Unit))).collect::<Vec<_>>();
    let bytes = postcard::to_allocvec(&data::Ref(&values)).unwrap();
    assert!(data::decode::<Vec<(Unit, ((), Unit))>>(&bytes).is_err());
    assert!(data::encode(&values).is_err());
    assert!(data::encode(&vec![(); usize::MAX]).is_err());

    for count in 500..700 {
        let values = vec![(); count];
        if let Ok(bytes) = data::encode(&values) {
            assert_eq!(data::decode::<Vec<()>>(&bytes).unwrap().len(), count);
        }
    }

    let encoded_count = postcard::to_allocvec(&u32::MAX).unwrap();
    assert!(data::decode::<Vec<()>>(&encoded_count).is_err());
    assert!(data::decode::<Vec<String>>(&encoded_count).is_err());
    assert!(data::decode::<bool>(&[7]).is_err());
    assert!(data::decode::<char>(&[255]).is_err());
    assert!(data::decode::<String>(&[1, 255]).is_err());
    assert!(data::decode::<Choice>(&[255]).is_err());
}
