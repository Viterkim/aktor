use super::{
    AktorMode, AktorNew, AktorStart, AktorStartContext, AktorStartError, AktorThreadFuture,
};
use crate::{AktorGroup, AktorSetupError};
use core::marker::PhantomData;

/// Return an actor setup from a helper without naming its closure types.
pub struct AktorDefinition<Handles> {
    setup: Box<dyn DefinitionStart<Handles> + Send>,
}

trait DefinitionStart<Handles> {
    fn begin(&self, group: &mut AktorGroup) -> Result<(), AktorSetupError>;
    fn start(
        self: Box<Self>,
        context: AktorStartContext<AktorGroup>,
    ) -> AktorThreadFuture<Result<Handles, AktorStartError>>;
}

struct DefinitionSetup<Setup, Mode> {
    setup: Setup,
    mode: PhantomData<fn() -> Mode>,
}
impl<Setup, Mode, Handles> DefinitionStart<Handles> for DefinitionSetup<Setup, Mode>
where
    Setup: AktorStart<
            Mode,
            Group = AktorGroup,
            Handles = Handles,
            Startup = AktorThreadFuture<Result<Handles, AktorStartError>>,
        >,
{
    fn begin(&self, group: &mut AktorGroup) -> Result<(), AktorSetupError> {
        self.setup.begin(group)
    }

    fn start(
        self: Box<Self>,
        context: AktorStartContext<AktorGroup>,
    ) -> AktorThreadFuture<Result<Handles, AktorStartError>> {
        self.setup.start_in(context)
    }
}

impl<S: 'static, Start, Kind: AktorMode, Role, End> AktorNew<S, Start, Kind, Role, End> {
    /// Hide the callback types when returning a native actor definition from a helper.
    pub fn definition<Mode, Handles>(self) -> AktorDefinition<Handles>
    where
        Self: AktorStart<
                Mode,
                Group = AktorGroup,
                Handles = Handles,
                Startup = AktorThreadFuture<Result<Handles, AktorStartError>>,
            > + Send
            + 'static,
        Mode: 'static,
    {
        AktorDefinition {
            setup: Box::new(DefinitionSetup {
                setup: self,
                mode: PhantomData,
            }),
        }
    }
}
impl<Handles> AktorStart for AktorDefinition<Handles> {
    type Group = AktorGroup;
    type Handles = Handles;
    type Startup = AktorThreadFuture<Result<Handles, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        self.setup.begin(group)
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        self.setup.start(context)
    }
}
