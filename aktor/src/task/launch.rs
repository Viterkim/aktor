use super::*;
#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
use crate::setup::kind::TokioTask;
use crate::{
    AktorClosure, AktorGroup, AktorGroupError, AktorHooks, AktorNew, AktorStartReason, LaunchError,
    owner_tasks::SendWork,
    serve::{self, Join, Registered, Serve, Started, TaskJoin},
    setup::{StartFailure, kind::AktorMode, validation_error},
};
use er::*;
use hooks::AktorTaskInterval;
use std::sync::Weak;
use tokio::sync::watch;

type TaskSyncLaunch<S, E> = Result<(AktorTask<S>, oneshot::Receiver<Started<E>>), StartFailure<E>>;

#[cfg(all(feature = "tokio", not(target_family = "wasm")))]
#[doc(hidden)]
pub(crate) fn start_sync<S, Start, Fut, End, CM, E>(
    setup: crate::setup::Prepared<S, Start, TokioTask, End, CM>,
    group: &mut AktorGroup,
) -> TaskSyncLaunch<S, E>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, (E, String)>> + Send + 'static,
    E: Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, CM>,
{
    // A group started by a StdThread actor has no runtime of its own to put the task on.
    if let Err(error) = tokio::runtime::Handle::try_current() {
        let failure = er_new!(LaunchError::new(AktorStartReason::InvalidSetup), error);
        return Err(validation_error(
            group,
            &setup.name,
            crate::AktorExecution::TokioTask,
            failure.into(),
        ));
    }

    start_sync_on(setup, group, Clock::Tokio, |future| {
        TaskJoin::Tokio(tokio::spawn(future))
    })
}

pub(crate) fn ready<E: Send + 'static>(
    mut started: oneshot::Receiver<Started<E>>,
    group: AktorGroup,
) -> crate::setup::AktorReady<AktorGroup, E> {
    Box::pin(async move {
        let kill = group.killswitch();
        let completion = group.completion();
        let started = tokio::select! {
            biased;
            started = &mut started => started.ok(),
            _ = completion.wait() => started.try_recv().ok(),
        };

        match started {
            Some(Started::Ready) => Ok(()),
            Some(Started::Init(failure)) => Err(failure.into()),
            Some(Started::Cut) => er_bail!(
                LaunchError::new(AktorStartReason::Stopping),
                AktorGroupError::Stopping
            ),
            None if kill.is_stopping() => er_bail!(
                LaunchError::new(AktorStartReason::Stopping),
                AktorGroupError::Stopping
            ),
            None => er_bail!(
                LaunchError::new(AktorStartReason::InvalidSetup),
                TaskSetupErr::Cancelled
            ),
        }
    })
}

#[derive(Er)]
enum TaskSetupErr {
    Cancelled,
}

pub(crate) fn start_sync_on<S, Start, Fut, Kind, End, CM, E>(
    setup: crate::setup::Prepared<S, Start, Kind, End, CM>,
    group: &mut AktorGroup,
    clock: Clock,
    spawn: impl FnOnce(TaskOwnerFuture) -> TaskJoin,
) -> TaskSyncLaunch<S, E>
where
    S: Send + 'static,
    Start: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<S, (E, String)>> + Send + 'static,
    E: Send + 'static,
    End: crate::setup::closures::AktorSendEnd<S, CM>,
    Kind: AktorMode<
            Leave = (),
            Each<S> = dyn FnMut(&mut S, Operation) + Send,
            Interval<S> = dyn AktorTaskInterval<S> + Send,
        >,
{
    let kind = Kind::EXECUTION;
    let capacity = setup.capacity;

    let invalid = || LaunchError::new(AktorStartReason::InvalidSetup);

    let (inner, mailbox) = match crate::queue::channel(capacity).er(invalid) {
        Ok(channel) => channel,
        Err(error) => return Err(validation_error(group, &setup.name, kind, error.into())),
    };

    let AktorNew {
        name,
        start,
        end,
        hooks,
        stuck,
        ..
    } = setup;
    let AktorHooks {
        intervals,
        before_each,
        after_each,
    } = hooks;
    let kill = group.killswitch();

    inner.admission.manage(kill.clone());

    let Registered {
        force,
        served,
        join,
    } = serve::register(group, name.clone(), kind, &inner, stuck)
        .er_with(|error| LaunchError::new(error.reason))?;

    #[cfg(not(target_family = "wasm"))]
    let life = kill.track_thread(name.clone(), stuck);
    let (tell, started) = oneshot::channel();
    let intervals = intervals
        .into_iter()
        .map(|interval| {
            let run = Arc::new(parking_lot::Mutex::new(interval.run));

            serve::Interval {
                every: interval.every,
                message: Box::new(move |due| interval_message(run.clone(), due))
                    as Box<dyn FnMut(serve::Due) -> Message<S> + Send>,
            }
        })
        .collect();

    // Made up front, so the group can still see the state after the task when a lease kept it.
    let owner = Arc::new(Mutex::new(None));
    let state = Arc::downgrade(&owner) as Weak<dyn core::any::Any + Send + Sync>;

    let actor = serve::serve::<S, Message<S>, SendWork, E, _, _, _, _>(Serve {
        report: serve::Report::new(
            served,
            name.clone(),
            kind,
            kill.clone(),
            mailbox.admission.clone(),
        ),
        name,
        kind,
        kill: kill.clone(),
        mailbox,
        handle: Arc::downgrade(&inner),
        force,
        start: async move {
            let state = start().await?;

            *owner.lock().await = Some(state);
            Ok(Leased { owner, lease: None })
        },
        end: move |state: Leased<S>| async move {
            match state.into_inner().await {
                Some(state) => end.finish(state).await,
                None => Ok(()),
            }
        },
        hooks: CallHooks {
            before_each,
            after_each,
        },
        intervals,
        stuck,
        clock,
        yields: shares_threads(clock),
        ready: move |started| {
            let _ = tell.send(started);
        },
        cleaned: watch::channel(None).0,
    });

    let task = spawn(Box::pin(async move {
        #[cfg(not(target_family = "wasm"))]
        let _life = life;

        actor.await;
    }));

    let _ = join.send(Join { task, state });

    let task = AktorTask {
        inner,
        kill,
        role: PhantomData,
        clock,
    };

    group.keep(task.clone());

    Ok((task, started))
}

fn interval_message<S: Send + 'static>(
    run: Arc<parking_lot::Mutex<AktorClosure<dyn AktorTaskInterval<S> + Send>>>,
    due: serve::Due,
) -> Message<S> {
    Message {
        operation: Operation::INTERVAL,
        job: Box::pin(IntervalJob {
            run,
            future: None,
            relock: None,
            _due: due,
        }),
    }
}

/// Tokio budgets its own tasks. The others share their threads with no budget, so the actor gives
/// them a turn now and then.
fn shares_threads(clock: Clock) -> bool {
    match clock {
        #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
        Clock::Tokio => false,
        #[cfg(all(feature = "std_thread", not(target_family = "wasm")))]
        Clock::Std => true,
        #[cfg(target_family = "wasm")]
        Clock::Browser => true,
    }
}
