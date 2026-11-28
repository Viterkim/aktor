use super::*;
use crate::AktorGroupStuck;
use er::*;

/// Every stopping group shares one watchdog thread. It cuts a group off at its deadline, and with
/// `AbortProcess` it prints what's still pending right before the kill.
struct Watchdog {
    watched: std::sync::Mutex<Watched>,
    wake: std::sync::Condvar,
}

struct Watched {
    groups: Vec<Watch>,
    poked: bool,
    started: bool,
}

#[derive(Clone)]
struct Watch {
    kill: AktorKillSwitch,
    /// Still has to enforce the deadline.
    guarding: bool,
    /// Still has to print what's pending, `AbortProcess` only.
    reporting: bool,
}

static WATCHDOG: Watchdog = Watchdog {
    watched: std::sync::Mutex::new(Watched {
        groups: Vec::new(),
        poked: false,
        started: false,
    }),
    wake: std::sync::Condvar::new(),
};

fn watched() -> std::sync::MutexGuard<'static, Watched> {
    WATCHDOG
        .watched
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

/// Something about a stopping group changed, the watchdog takes another look.
pub fn poke() {
    let mut watched = watched();

    if watched.groups.is_empty() {
        return;
    }

    watched.poked = true;
    drop(watched);
    WATCHDOG.wake.notify_all();
}

#[derive(Er)]
#[er(format = "could not start the shutdown watchdog")]
struct StartWatchdogErr;
fn start_watchdog() -> ErResult<(), StartWatchdogErr> {
    std::thread::Builder::new()
        .name("aktor watchdog".into())
        .spawn(watch)
        .map(drop)
        .er(())
}

pub fn watchdog(kill: &AktorKillSwitch) {
    let started = {
        let mut watched = watched();

        watched.groups.push(Watch {
            kill: kill.clone(),
            guarding: true,
            reporting: kill.control.shutdown_policy != AktorGroupStuck::KeepRunning,
        });
        watched.poked = true;

        let started = match watched.started {
            true => Ok(()),
            false => start_watchdog(),
        };

        match started {
            Ok(()) => watched.started = true,
            Err(_) => drop(watched.groups.pop()),
        }

        started
    };

    WATCHDOG.wake.notify_all();

    let Err(error) = started else {
        return;
    };

    let (report, abandoned, deadline) = {
        let mut state = kill.control.lock();
        state.report.record(AktorFailureInput::new(
            crate::AktorFailureSource::GroupDriver,
            error.er_report(),
        ));
        let abandoned = state.watchdog_disposal == WatchdogDisposal::Requested;
        state.watchdog_disposal = WatchdogDisposal::Gone;
        if abandoned {
            state.abandon_shutdown_hooks();
        }

        (state.report.snapshot(), abandoned, state.deadline)
    };

    if kill.control.shutdown_policy == AktorGroupStuck::AbortProcess(()) {
        let printed = print_report(move || report.render(None));
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let _ = printed.recv_timeout(remaining.min(super::impls::reserve(kill.control.grace)));
        }
        terminate();
    }

    if abandoned {
        kill.publish_stopped();
    } else {
        kill.control.publish_pending();
    }
}

fn watch() {
    let mut watched = watched();

    loop {
        // Each group gets looked at under its own lock only, a group waking the watchdog while
        // holding it can't deadlock against this.
        watched.poked = false;
        let groups = watched.groups.clone();
        drop(watched);

        let mut next: Option<Instant> = None;
        let mut stepped = Vec::with_capacity(groups.len());

        for mut group in groups {
            for (active, step) in [
                (
                    &mut group.guarding,
                    guard as fn(&AktorKillSwitch) -> Option<Instant>,
                ),
                (&mut group.reporting, report),
            ] {
                if !*active {
                    continue;
                }

                match step(&group.kill) {
                    Some(at) => next = Some(next.map_or(at, |next| next.min(at))),
                    None => *active = false,
                }
            }

            stepped.push(group);
        }

        watched = self::watched();
        watched.groups.retain_mut(|group| {
            let done = stepped
                .iter()
                .find(|done| Arc::ptr_eq(&done.kill.control, &group.kill.control));

            if let Some(done) = done {
                group.guarding &= done.guarding;
                group.reporting &= done.reporting;
            }

            group.guarding || group.reporting
        });

        if watched.poked {
            continue;
        }

        watched = match next {
            Some(at) => {
                let remaining = at.saturating_duration_since(Instant::now());

                WATCHDOG
                    .wake
                    .wait_timeout(watched, remaining)
                    .unwrap_or_else(|error| error.into_inner())
                    .0
            }
            None => WATCHDOG
                .wake
                .wait(watched)
                .unwrap_or_else(|error| error.into_inner()),
        };
    }
}

/// Cuts the group off at its deadline. `None` once there's nothing left to guard.
fn guard(kill: &AktorKillSwitch) -> Option<Instant> {
    let control = &kill.control;
    let mut state = control.lock();

    if settled(&state)
        || (control.shutdown_policy == AktorGroupStuck::KeepRunning
            && state.finished
            && !state.callback_pending())
    {
        state.watchdog_disposal = WatchdogDisposal::Gone;
        return None;
    }

    if state.watchdog_disposal == WatchdogDisposal::Requested {
        state.watchdog_disposal = WatchdogDisposal::Gone;
        drop(state);

        // Only asked for when no thread could be started to drop them. This drop can block the
        // watchdog, so the report goes out first.
        control.publish_pending();
        kill.drop_shutdown_hooks();
        return None;
    }

    // Shutdown pulls the deadline in once every actor had its turn.
    let deadline = state.deadline.unwrap_or_else(Instant::now);

    if Instant::now() < deadline {
        return Some(deadline);
    }

    if control.shutdown_policy == AktorGroupStuck::AbortProcess(()) {
        drop(state);
        terminate();
    }

    if state.applications != 0 {
        state.report.late(crate::AktorLateWork::Application);
    }
    if state.callback_pending() {
        state.report.late(crate::AktorLateWork::ShutdownCallback);
    }
    if !state.actors_and_hooks_done {
        let owner_pending = state
            .report
            .actors
            .iter()
            .any(|actor| actor.retirement() == Some(crate::AktorRetirement::StillRunning));
        if !owner_pending && state.applications == 0 && !state.callback_pending() {
            state.report.late(crate::AktorLateWork::GroupDriver);
        }

        for actor in &mut state.report.actors {
            if let Some(owner) = &mut actor.owner
                && owner.retirement == crate::AktorRetirement::StillRunning
            {
                owner.ran_past_grace = true;
            }
        }
    }

    state.watchdog_disposal = WatchdogDisposal::Gone;
    drop(state);
    control.publish_pending();
    None
}

/// Says what's still pending a little before an `AbortProcess` group gets killed.
fn report(kill: &AktorKillSwitch) -> Option<Instant> {
    let reserve = super::impls::reserve(kill.control.grace);
    let state = kill.control.lock();

    if settled(&state) {
        return None;
    }

    let deadline = state.deadline.unwrap_or_else(Instant::now);
    let reporting_at = deadline.checked_sub(reserve / 8).unwrap_or(deadline);

    if Instant::now() < reporting_at {
        return Some(reporting_at);
    }

    let report = state.report.snapshot();
    let application_work = state.applications != 0;
    let callback_running = state.callback_pending();

    drop(state);

    print_report(move || {
        let mut pending = ErBranch::from(Pending::Group);
        let details = report.render(None);
        if !details.is_empty() {
            pending = pending.er_save("shutdown report", details);
        }
        if application_work {
            pending = pending.er_add(Pending::Application);
        }
        if callback_running {
            pending = pending.er_add(Pending::Callback);
        }

        pending.er_report()
    });
    None
}

#[derive(Er)]
enum Pending {
    #[er(format = "waiting for the group to finish shutdown")]
    Group,
    #[er(format = "waiting for application work")]
    Application,
    #[er(format = "waiting for the shutdown callback")]
    Callback,
}

fn print_report(render: impl FnOnce() -> String + Send + 'static) -> std::sync::mpsc::Receiver<()> {
    let (done, printed) = std::sync::mpsc::channel();

    // Someone else can hold stderr. Printing here would keep the watchdog from the kill.
    let _printing = std::thread::Builder::new()
        .name("aktor shutdown report".into())
        .spawn(move || {
            eprintln!("{}", render());
            let _ = done.send(());
        });

    printed
}

fn settled(state: &State) -> bool {
    (state.finished || state.actors_and_hooks_done)
        && !state.callback_pending()
        && state.applications == 0
        && state.only_left()
}

impl Control {
    fn publish_pending(&self) {
        self.completed.send_if_modified(|completed| {
            if completed.is_some() {
                return false;
            }

            let report = Arc::new(self.lock().report.snapshot());
            *completed = Some(Completed {
                grace: report.clone(),
                report,
                collection: Collection::Pending,
                work_finished: false,
            });
            true
        });
    }
}

fn terminate() -> ! {
    #[cfg(unix)]
    {
        use rustix::process::{Signal, getpid, kill_process};

        // Even a stuck abort handler cannot catch SIGKILL.
        let _ = kill_process(getpid(), Signal::KILL);
    }
    std::process::abort();
}

impl AktorKillSwitch {
    #[doc(hidden)]
    pub fn track_thread(&self, name: String, stuck: crate::AktorStuck) -> ThreadLife {
        let name = Arc::new(name);

        self.control.lock().running.push((name.clone(), stuck));
        ThreadLife {
            control: self.control.clone(),
            name,
        }
    }
}

#[derive(Er)]
#[er(format = "the cancelled shutdown closure could not be dropped")]
struct AbandonedShutdown;

impl State {
    fn callback_pending(&self) -> bool {
        self.callback_running || self.callback_workers != 0
    }

    pub fn abandon_shutdown_hooks(&mut self) {
        // Both shutdown threads are unavailable. The caller cannot safely run this destructor.
        std::mem::forget(self.listener_hook.take());
        std::mem::forget(self.shutdown_hook.take());
        self.callback_running = false;
        self.report.record(AktorFailureInput::new(
            crate::AktorFailureSource::GroupDriver,
            er_new!(AbandonedShutdown).er_report(),
        ));
    }

    /// Every thread still running belongs to an actor shutdown may leave behind.
    pub fn only_left(&self) -> bool {
        self.running
            .iter()
            .all(|(_, stuck)| *stuck == crate::AktorStuck::Leave)
    }
}

impl Drop for ThreadLife {
    fn drop(&mut self) {
        let mut state = self.control.lock();

        state
            .running
            .retain(|(name, _)| !Arc::ptr_eq(name, &self.name));

        drop(state);
        poke();
        self.control.threads.notify_one();
    }
}
