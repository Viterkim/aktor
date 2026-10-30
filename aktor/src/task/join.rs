#[cfg(feature = "bevy")]
use crate::group::shutdown::panic_message;
use crate::{
    AktorError, AktorFailureReport, AktorOutcome, KillSwitch, group::shutdown::contain_drop,
};
#[cfg(feature = "bevy")]
use futures_util::FutureExt;

pub enum TaskJoin {
    #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
    Tokio(tokio::task::JoinHandle<()>),
    #[cfg(feature = "bevy")]
    Bevy(bevy_tasks::Task<()>),
}
impl TaskJoin {
    pub async fn wait(self, kill: &KillSwitch, report: &mut AktorOutcome) {
        match self {
            #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
            Self::Tokio(join) => match join.await {
                Ok(()) => {}
                Err(error) => {
                    let message = error.to_string();

                    record(kill, report, message);
                    contain_drop(error, kill, "task join error drop");
                }
            },
            #[cfg(feature = "bevy")]
            Self::Bevy(join) => match std::panic::AssertUnwindSafe(join).catch_unwind().await {
                Ok(()) => {}
                Err(payload) => {
                    let message = panic_message(&payload);

                    record(kill, report, message);
                    contain_drop(payload, kill, "task join panic drop");
                }
            },
        }
    }
}

fn record(kill: &KillSwitch, report: &mut AktorOutcome, message: String) {
    kill.fail(AktorFailureReport {
        actor: report.actor.clone(),
        kind: report.kind,
        phase: "owner".into(),
        message: message.clone(),
    });

    report.diagnostics.push(AktorError::new(message, ()));
}
