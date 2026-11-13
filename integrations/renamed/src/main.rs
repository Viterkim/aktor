use actor::listener::spawn_local;
use actor::*;
use er::*;
use std::time::Duration;

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> ErMain {
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
            .any(|name| name.contains("[aktor-data]"))
    );

    let bytes = actor::data::encode(&Update {
        by: 7,
        cached: "local".into(),
    })?;
    assert_eq!(
        actor::data::decode::<Update>(&bytes)?,
        Update {
            by: 7,
            cached: String::new()
        }
    );

    let executor = tokio::task::LocalSet::new();

    executor
        .run_until(async {
            let (state, actor) = spawn_local(&executor, State::default(), 8)?;

            assert_eq!(add(&state, 2).await, 2);

            drop(state);
            actor.await?;

            let actors = actor::aktor_start(AktorSetup {
                actors: AktorNew {
                    name: "named counter".into(),
                    kind: AktorKind::TokioTask,
                    start: async || aktor_ready(State::default()),
                    end: (),
                    hooks: Default::default(),
                    capacity: 32,
                    stuck: AktorStuck::Kill,
                },
                shutdown: |_| {},
                grace: Duration::from_secs(5),
            })
            .await
            .er_report()?;

            assert_eq!(add(&actors.handles, 85).await, 85);

            let input = Update {
                by: 1,
                cached: "kept locally".into(),
            };
            assert_eq!(
                update(&actors.handles, input.clone()).await,
                Update {
                    by: 86,
                    cached: input.cached.clone()
                }
            );

            let (sender, mut results) = update(&actors.handles, input).latest();
            assert_eq!(results.next().await.unwrap().by, 87);
            drop((sender, results));

            assert!(!actors.shutdown().await.failed());

            Ok(())
        })
        .await
}
