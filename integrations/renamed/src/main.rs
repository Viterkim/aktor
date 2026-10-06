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

#[derive(AktorData, Debug, Clone, PartialEq)]
pub struct Update {
    pub by: usize,
    #[aktor(skip)]
    pub cached: String,
}

#[aktor(data)]
pub async fn update(state: &mut State, input: Update) -> Update {
    state.value += input.by;
    Update {
        by: state.value,
        cached: input.cached,
    }
}

#[derive(Er)]
struct MainError;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ErReport<MainError>> {
    let operations = actor::worker::Operations::for_actor::<State, ()>();

    assert_eq!(operations.0.len(), 2);
    assert!(
        operations
            .0
            .iter()
            .any(|name| name.contains("renamed::add"))
    );
    assert!(
        operations
            .0
            .iter()
            .any(|name| name.contains("aktor-data-postcard-v1"))
    );

    let bytes = actor::data::encode(&Update {
        by: 7,
        cached: "local".into(),
    })
    .er(())?;
    assert_eq!(
        actor::data::decode::<Update>(&bytes).er(())?,
        Update {
            by: 7,
            cached: String::new()
        }
    );

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

            let input = Update {
                by: 1,
                cached: "kept locally".into(),
            };
            assert_eq!(
                update(&actors.handles.counter, input.clone()).await,
                Update {
                    by: 86,
                    cached: input.cached.clone()
                }
            );

            let (sender, mut results) = update(&actors.handles.counter, input).latest();
            assert_eq!(results.next().await.unwrap().by, 87);
            drop((sender, results));

            assert!(!actors.shutdown().await.failed());

            Ok(())
        })
        .await
}
