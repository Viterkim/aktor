use actor::listener::spawn_local;
use actor::*;
use std::error::Error;

#[derive(Default)]
pub struct State {
    pub value: usize,
}

#[aktor]
pub async fn add(state: &mut State, value: usize) -> usize {
    state.value += value;

    state.value
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let (state, actor) = spawn_local(&executor, State::default(), 8)?;
            assert_eq!(add(&state, 2).await, 2);

            drop(state);
            actor.await?;

            Ok(())
        })
        .await
}
