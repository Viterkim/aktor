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
}

#[test]
fn portable_values() {
    let value = Portable {
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
    };
    let bytes = worker::encode(&value).unwrap();
    let result: Portable = worker::decode(&bytes).unwrap();
    assert_eq!(result, value);
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
