use super::*;

impl AktorName {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl<S: 'static, Start, Kind: AktorMode> AktorClosures<S, Start, Kind> {
    pub fn new(start: Start) -> Self {
        Self {
            start,
            end: None,
            intervals: Vec::new(),
            before_each: None,
            after_each: None,
        }
    }
}

impl<S: 'static, Start, Kind: AktorMode, Role> AktorSetup<S, Start, Kind, Role> {
    pub fn with_role<NewRole>(self, role: NewRole) -> AktorSetup<S, Start, Kind, NewRole> {
        AktorSetup {
            name: self.name,
            role,
            kind: self.kind,
            closures: self.closures,
            options: self.options,
        }
    }

    pub fn execution(&self) -> AktorExecution {
        Kind::EXECUTION
    }

    pub async fn start_in(
        self,
        group: &<Self as AktorStart>::Group,
    ) -> Result<<Self as AktorStart>::Handles, AktorStartupError<<Self as AktorStart>::Error>>
    where
        Self: AktorStart,
    {
        start_in(group, self).await
    }

    pub fn start(self) -> AktorStartup<Self>
    where
        Self: AktorStart,
    {
        start(self)
    }
}

impl Default for AktorOptions {
    fn default() -> Self {
        Self {
            capacity: 32,
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

impl<Handles, Group: AktorSetupGroup> AktorStarted<Handles, Group> {
    pub fn killswitch(&self) -> Group::KillSwitch {
        self.group.killswitch()
    }

    pub fn completion(&self) -> Group::Completion {
        self.group.completion()
    }

    pub fn shutdown(&self) -> Group::Closing {
        Group::stop(&self.group.killswitch());
        self.group.closing()
    }
}

impl<Group: AktorSetupGroup> Clone for AktorStartContext<Group> {
    fn clone(&self) -> Self {
        Self {
            group: self.group.registration(),
        }
    }
}

pub fn start<Setup: AktorStart>(setup: Setup) -> AktorStartup<Setup> {
    let group = Setup::Group::new(setup.grace());

    AktorStartup {
        killswitch: group.killswitch(),
        completion: group.completion(),
        begin: true,
        group: Some(group),
        setup: Some(Box::new(setup)),
        starting: None,
        failure: None,
        shutdown: None,
    }
}

/// Add prepared actors to a group whose shutdown listener is already running.
/// Failure or cancellation after startup begins stops the whole group, including existing actors.
pub async fn start_in<Setup: AktorStart>(
    group: &Setup::Group,
    setup: Setup,
) -> Result<Setup::Handles, AktorStartupError<Setup::Error>> {
    group.check_started().map_err(|error| AktorStartupError {
        error: Setup::Error::from(error),
        report: None,
    })?;

    let startup = AktorStartup {
        begin: false,
        killswitch: group.killswitch(),
        completion: group.completion(),
        group: Some(group.registration()),
        setup: Some(Box::new(setup)),
        starting: None,
        failure: None,
        shutdown: None,
    };

    startup.await.map(|started| started.handles)
}

impl From<AktorSetupError> for AktorStartError {
    fn from(error: AktorSetupError) -> Self {
        Self::Setup(error)
    }
}
