use super::*;
use crate::{
    latest::{SendLatest, Session},
    listener::channel,
    operation::Operation,
};
use futures_util::FutureExt;

#[tokio::test]
async fn shutdown() {
    for input in [None, Some(9)] {
        let (handle, mut listener) = channel::<()>(1).unwrap();
        let (sender, results) = (&handle).session(
            Operation {
                name: "shutdown",
                caller: core::panic::Location::caller(),
            },
            async |_, input: u32| input,
        );
        if let Some(input) = input {
            sender.send(input);
        }

        let mut results = LatestResults::new(results, handle.completion());
        results.closed = Some(Box::pin(async move {
            listener.close();
            listener.serve(&mut ()).now_or_never().unwrap();
        }));

        assert_eq!(results.next().await, input);
        assert_eq!(results.next().await, None);
    }
}
