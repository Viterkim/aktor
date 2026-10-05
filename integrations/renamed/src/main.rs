use actor::listener::spawn_local;
use actor::*;
use er::*;

#[derive(Default)]
pub struct State {
    pub value: usize,
}

#[aktor]
pub async fn add(state: &mut State, value: usize) -> usize {
    state.value += value;

    state.value
}

#[derive(Er)]
struct MainError;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ErReport<MainError>> {
    let operations = actor::worker::Operations::for_actor::<State, ()>();

    assert_eq!(operations.0.len(), 1);
    assert!(operations.0[0].contains("renamed::add"));

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let (state, actor) = spawn_local(&executor, State::default(), 8).er(())?;

            assert_eq!(add(&state, 2).await, 2);

            drop(state);
            actor.await.er(())?;

            let actors = actor::start(aktor_setups! {
                counter: AktorSetup {
                    name: AktorName::new("named counter"),
                    role: AktorNoRole,
                    kind: AktorKind::TokioTask,
                    closures: AktorClosures {
                        start: async || Ok(State::default()),
                        end: None,
                        intervals: vec![],
                        before_each: None,
                        after_each: None,
                    },
                    options: None,
                },
            })
            .await
            .er(())?;

            assert_eq!(add(&actors.handles.counter, 85).await, 85);
            assert!(!actors.shutdown().await.failed());

            Ok(())
        })
        .await
}
