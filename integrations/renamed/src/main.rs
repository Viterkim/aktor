use actor::*;
#[cfg(not(target_family = "wasm"))]
use er::*;
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

#[derive(Default)]
pub struct State {
    #[cfg(not(target_family = "wasm"))]
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

#[aktor(latest)]
pub async fn update(state: &mut State, input: Update) -> Update {
    state.value += input.by;
    Update {
        by: state.value,
        cached: input.cached,
    }
}

impl AktorWorkerContract for State {
    type State = Self;
    type Role = ();
    type Config = ();
    type Ready = ();
    type Error = ();
}

#[cfg(target_family = "wasm")]
pub async fn call_page(worker: &worker::Worker<State>, input: Update) {
    let (sender, _) = update(worker, input).latest();
    let sender: update::Sender = sender;
    sender.send(Update {
        by: 1,
        cached: String::new(),
    });
    add(worker, 1).await.unwrap();
}

#[cfg(target_family = "wasm")]
fn main() {}

#[cfg(not(target_family = "wasm"))]
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
    })
    .er(())?;
    assert_eq!(
        actor::data::decode::<Update>(&bytes).er(())?,
        Update {
            by: 7,
            cached: String::new()
        }
    );

    let actors = actor::aktor_start(AktorSetup {
        stuck: actor::AktorGroupStuck::KeepRunning,
        actors: AktorNew {
            name: "named counter".into(),
            kind: AktorKind::TokioTask,
            start: actor::AktorInit::Ready(async || State::default()),
            end: (),
            hooks: AktorHooks::NONE,
            capacity: core::num::NonZeroUsize::new(32).unwrap(),
            stuck: AktorStuck::Kill,
        },
        shutdown: |_| {},
        grace: Duration::from_secs(5),
    });
    actors
        .ready()
        .await
        .er_val(|(_, report)| ErMainError.er_save("startup", report))?;

    assert_eq!(add(actors.handles(), 85).await.unwrap(), 85);

    let input = Update {
        by: 1,
        cached: "kept locally".into(),
    };
    assert_eq!(
        update(actors.handles(), input.clone()).await.unwrap(),
        Update {
            by: 86,
            cached: input.cached.clone()
        }
    );

    let (sender, mut results) = update(actors.handles(), input).latest();
    assert_eq!(results.next().await.unwrap().unwrap().by, 87);
    drop((sender, results));

    assert!(!actors.stop().await.failed());

    Ok(())
}
