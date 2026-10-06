use aktor::{
    AktorData, aktor, data,
    dispatch::DataCodec,
    message::CallError,
    worker::{self, Codec},
};

#[derive(AktorData, Debug, PartialEq)]
struct Record {
    key: String,
    bytes: Vec<u8>,
    #[aktor(skip)]
    local: usize,
}

#[derive(Default)]
struct State(usize);

#[aktor(data)]
async fn store(state: &mut State, record: Record) -> Result<Record, String> {
    state.0 += 1;
    Ok(record)
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

    assert!(bytes.len() < 1040);
    let output = worker::dispatch::<State, ()>(&mut state, store::NAME.into(), bytes.clone())
        .await
        .unwrap();
    let decoded: Result<Record, String> = data::decode(&output).unwrap();
    assert_eq!(decoded.unwrap().local, 0);
    assert_eq!(state.0, 1);

    let error = worker::dispatch::<State, ()>(&mut state, store::NAME.into(), bytes[..4].to_vec())
        .await
        .unwrap_err();
    assert_eq!(error.outcome, CallError::Discarded);
    assert_eq!(state.0, 1);

    let error = <DataCodec as Codec<Record>>::decode_output(&[255]).unwrap_err();
    assert_eq!(error.outcome, CallError::OutcomeUnknown);
    assert!(
        worker::Operations::for_actor::<State, ()>().0[0].ends_with("[aktor-data-postcard-v1]")
    );
}
