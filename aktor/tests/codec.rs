#![cfg(feature = "wasm_browser_workers")]
use aktor::{AktorCleanupError, worker};
use er::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Choice {
    Name { name: String },
    Number(u64),
    Tagged(Tagged),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum Tagged {
    Empty(()),
    Unit,
    Pair(u8, u8),
    Fields { value: u8 },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Portable {
    options: Vec<Option<Option<u8>>>,
    units: Vec<Option<()>>,
    map: BTreeMap<(u32, String), u128>,
    choice: Vec<Choice>,
    cleanup: AktorCleanupError<Vec<u32>>,
    limits: (i128, u128, char),
    numbers: (i8, u8, i16, u16, i32, u32, i64, u64, f32, f64),
}

#[test]
fn portable_values() {
    let mut value = Portable {
        options: vec![None, Some(None), Some(Some(7))],
        units: vec![None, Some(())],
        map: BTreeMap::from([((7, "key".into()), u128::MAX)]),
        choice: vec![
            Choice::Name { name: "cat".into() },
            Choice::Number(u64::MAX),
            Choice::Tagged(Tagged::Empty(())),
            Choice::Tagged(Tagged::Unit),
            Choice::Tagged(Tagged::Pair(7, 17)),
            Choice::Tagged(Tagged::Fields { value: 7 }),
        ],
        cleanup: AktorCleanupError {
            diagnostics: "first\nsecond".into(),
            data: vec![7, 17],
        },
        limits: (i128::MIN, u128::MAX, 'ø'),
        numbers: (
            i8::MIN,
            u8::MAX,
            i16::MIN,
            u16::MAX,
            i32::MIN,
            u32::MAX,
            i64::MIN,
            u64::MAX,
            f32::MIN,
            f64::MAX,
        ),
    };

    let bytes = worker::encode(&value).unwrap();
    let result: Portable = worker::decode(&bytes).unwrap();

    assert_eq!(result, value);

    for length in 0..bytes.len() {
        assert!(worker::decode::<Portable>(&bytes[..length]).is_err());
    }

    let mut extra = bytes.clone();

    extra.push(0);
    assert!(worker::decode::<Portable>(&extra).is_err());

    for tag in 0..=u8::MAX {
        let mut broken = bytes.clone();

        broken[0] = tag;

        if let Ok(value) = worker::decode::<Portable>(&broken) {
            assert_eq!(
                worker::decode::<Portable>(&worker::encode(&value).unwrap()).unwrap(),
                value
            );
        }
    }

    for size in [0, 1, 2, 7, 31, 128] {
        value.options = (0..size)
            .map(|index| match index % 3 {
                0 => None,
                1 => Some(None),
                _ => Some(Some(index as u8)),
            })
            .collect();
        value.map = (0..size)
            .map(|index| {
                (
                    (index as u32, format!("ø{index}")),
                    u128::MAX - index as u128,
                )
            })
            .collect();
        value.choice = (0..size)
            .map(|index| match index % 4 {
                0 => Choice::Tagged(Tagged::Unit),
                1 => Choice::Tagged(Tagged::Pair(index as u8, 17)),
                2 => Choice::Name {
                    name: format!("Kat {index}"),
                },
                _ => Choice::Number(u64::MAX - index as u64),
            })
            .collect();

        let bytes = worker::encode(&value).unwrap();

        assert_eq!(worker::decode::<Portable>(&bytes).unwrap(), value);

        for length in [0, bytes.len() / 2, bytes.len() - 1] {
            assert!(worker::decode::<Portable>(&bytes[..length]).is_err());
        }
    }
}

#[derive(Er, Serialize, Deserialize, PartialEq)]
pub enum PublicKind {
    Failed(u32),
}
#[derive(Er, Serialize, Deserialize, PartialEq)]
pub struct PublicError {
    #[er(into_top)]
    pub kind: PublicKind,
    #[er(into_report_string)]
    pub diagnostics: String,
    #[er(into_snapshot)]
    pub snapshot: ErSnapshot,
}

#[test]
fn portable_diagnostics() {
    let tree = ErTree::new(PublicKind::Failed(17), ["backup folder is read only"]);
    let result: Result<(), PublicError> = Err::<(), _>(tree).er_into(|_| {});
    let value = AktorCleanupError {
        diagnostics: "could not flush".into(),
        data: result.unwrap_err(),
    };
    let decoded: AktorCleanupError<PublicError> =
        worker::decode(&worker::encode(&value).unwrap()).unwrap();

    assert_eq!(decoded, value);
    assert!(
        decoded
            .data
            .diagnostics
            .contains("backup folder is read only")
    );
}

#[test]
fn large_bytes() {
    let value: Vec<u8> = (0u8..=255).cycle().take(8 * 1024 * 1024).collect();
    let bytes = worker::encode(&value).unwrap();

    assert!(bytes.len() < value.len() * 3);

    let decoded: Vec<u8> = worker::decode(&bytes).unwrap();

    assert_eq!(decoded, value);
}

#[test]
fn malformed_values() {
    let values = [
        vec![],
        vec![255],
        vec![19],
        vec![20],
        vec![22],
        vec![18],
        vec![23],
    ];

    for bytes in values {
        assert!(worker::decode::<Vec<u8>>(&bytes).is_err());
        assert!(worker::decode::<BTreeMap<String, u8>>(&bytes).is_err());
        assert!(worker::decode::<Tagged>(&bytes).is_err());
    }

    let mut nested = vec![18; 64];

    nested.push(255);
    assert!(worker::decode::<Option<Option<Option<u8>>>>(&nested).is_err());

    let mut sequence = worker::encode(&vec![1u8, 2]).unwrap();

    sequence.pop();
    assert!(worker::decode::<Vec<u8>>(&sequence).is_err());

    let mut map = worker::encode(&BTreeMap::from([("kat", 7u8)])).unwrap();

    map.pop();
    assert!(worker::decode::<BTreeMap<String, u8>>(&map).is_err());

    let mut variant = worker::encode(&Tagged::Pair(1, 2)).unwrap();

    variant.pop();
    assert!(worker::decode::<Tagged>(&variant).is_err());

    #[derive(Serialize, Deserialize)]
    enum Payload {
        Unit(u8),
    }
    #[derive(Serialize, Deserialize)]
    enum Tuple {
        Unit(u8, u8),
    }
    #[derive(Serialize, Deserialize)]
    enum Fields {
        Unit { value: u8 },
    }

    for marker in [
        worker::encode(&Tagged::Unit).unwrap(),
        worker::encode(&"Unit").unwrap(),
    ] {
        let mut value = marker.clone();

        value.extend(worker::encode(&7u8).unwrap());
        assert!(worker::decode::<Payload>(&value).is_err());

        let mut value = marker.clone();

        value.extend(worker::encode(&(7u8, 8u8)).unwrap());
        assert!(worker::decode::<Tuple>(&value).is_err());

        let mut value = marker;

        value.extend(worker::encode(&BTreeMap::from([("value", 7u8)])).unwrap());
        assert!(worker::decode::<Fields>(&value).is_err());
    }

    let payload = worker::encode(&Payload::Unit(7)).unwrap();

    assert!(matches!(
        worker::decode::<Payload>(&payload),
        Ok(Payload::Unit(7))
    ));
}

#[test]
fn nesting() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Nested {
        End,
        More(Box<Nested>),
        Tuple(Option<Box<Nested>>, ()),
        Fields { next: Option<Box<Nested>> },
    }

    for depth in [0, 1, 126, 127, 128, 129] {
        let value = (0..depth).fold(Nested::End, |next, _| Nested::More(Box::new(next)));

        if depth < 128 {
            let bytes = worker::encode(&value).unwrap();
            assert_eq!(worker::decode::<Nested>(&bytes).unwrap(), value);
        } else {
            assert!(worker::encode(&value).is_err());
        }
    }

    for fields in [false, true] {
        for depth in 0..=44 {
            let value = (0..depth).fold(Nested::End, |next, _| {
                if fields {
                    Nested::Fields {
                        next: Some(Box::new(next)),
                    }
                } else {
                    Nested::Tuple(Some(Box::new(next)), ())
                }
            });

            match worker::encode(&value) {
                Ok(bytes) => assert_eq!(worker::decode::<Nested>(&bytes).unwrap(), value),
                Err(_) => assert!(depth >= 43),
            }
        }
    }

    fn nested(depth: usize, tag: u8) -> Vec<u8> {
        let mut bytes = Vec::new();

        for _ in 0..depth {
            bytes.push(tag);

            match tag {
                19 => bytes.extend(1u64.to_le_bytes()),
                20 => bytes.extend(worker::encode(&"field").unwrap()),
                22 => {
                    bytes.extend(1u64.to_le_bytes());
                    bytes.push(b'V');
                }
                _ => {}
            }
        }

        bytes.extend(worker::encode(&()).unwrap());

        if tag == 19 || tag == 20 {
            bytes.extend(std::iter::repeat_n(23, depth));
        }

        bytes
    }

    for tag in [18, 19, 20, 21, 22] {
        assert!(worker::decode::<serde::de::IgnoredAny>(&nested(127, tag)).is_ok());

        for depth in [128, 4096] {
            assert!(worker::decode::<serde::de::IgnoredAny>(&nested(depth, tag)).is_err());
        }

        let bytes = nested(8, tag);

        for length in 0..bytes.len() {
            assert!(worker::decode::<serde::de::IgnoredAny>(&bytes[..length]).is_err());
        }
    }

    #[derive(Deserialize)]
    struct Known {
        value: u8,
    }

    for depth in [126, 127] {
        let mut bytes = vec![20];

        bytes.extend(worker::encode(&"unknown").unwrap());
        bytes.extend(nested(depth, 19));
        bytes.extend(worker::encode(&"value").unwrap());
        bytes.extend(worker::encode(&7u8).unwrap());
        bytes.push(23);

        match worker::decode::<Known>(&bytes) {
            Ok(value) => {
                assert_eq!(depth, 126);
                assert_eq!(value.value, 7);
            }
            Err(_) => assert_eq!(depth, 127),
        }
    }

    let mut hinted = worker::encode(&vec![7u8]).unwrap();

    hinted[1..9].copy_from_slice(&17u64.to_le_bytes());
    assert_eq!(worker::decode::<Vec<u8>>(&hinted).unwrap(), vec![7]);
}
