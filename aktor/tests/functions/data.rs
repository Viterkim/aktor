use aktor::{
    AktorData, aktor, data,
    dispatch::DataCodec,
    message::AktorDeliveryOutcome,
    worker::{self, Codec},
};

use er::*;

#[derive(AktorData, Debug, PartialEq)]
struct Record {
    key: String,
    bytes: Vec<u8>,
    #[aktor(skip)]
    local: usize,
}

#[derive(Default)]
struct State(usize);

#[derive(AktorData, Er)]
struct EmptyKey;

#[aktor]
async fn store(state: &mut State, record: Record) -> Result<Record, (EmptyKey, String)> {
    state.0 += 1;
    (!record.key.is_empty()).then_some(record).er(())
}

#[tokio::test]
async fn dispatch() {
    let mut state = State::default();
    let input = Record {
        key: "config".into(),
        bytes: vec![37; 1024],
        local: 42,
    };
    let bytes = data::encode(&(input,)).unwrap();

    let registry = worker::Registry::for_actor::<State, ()>().unwrap();
    let output = registry
        .dispatch(&mut state, store::NAME.into(), bytes.clone())
        .await
        .unwrap();
    let decoded: Result<Record, (EmptyKey, String)> = data::decode(&output).unwrap();
    assert_eq!(decoded.unwrap().local, 0);
    assert_eq!(state.0, 1);

    let error = registry
        .dispatch(&mut state, store::NAME.into(), bytes[..4].to_vec())
        .await
        .unwrap_err();
    assert_eq!(error.0.outcome, AktorDeliveryOutcome::Discarded);
    assert_eq!(state.0, 1);

    let error = <DataCodec as Codec<Record>>::decode(&[255]).unwrap_err();
    assert!(matches!(*error.0.reason, data::DataReason::Truncated));
    assert!(worker::Operations::for_actor::<State, ()>().0[0].ends_with("[aktor-data]"));
}
