use super::{
    AktorMode, AktorNew, AktorStart, AktorStartContext, AktorStartError, AktorThreadFuture,
};
use crate::{AktorGroup, AktorSetupError};
use core::marker::PhantomData;

/// A native actor definition with its callback types hidden.
/// Boxes the configuration once. Lifecycle failures use Aktor reports.
pub struct AktorDefinition<Actor> {
    setup: Box<dyn DefinitionStart<Actor> + Send>,
}

trait DefinitionStart<Actor> {
    fn begin(&self, group: &mut AktorGroup) -> Result<(), AktorSetupError>;
    fn start(
        self: Box<Self>,
        context: AktorStartContext<AktorGroup>,
    ) -> AktorThreadFuture<Result<Actor, AktorStartError>>;
}

struct DefinitionSetup<Setup, Mode> {
    setup: Setup,
    mode: PhantomData<fn() -> Mode>,
}
impl<Setup, Mode, Actor> DefinitionStart<Actor> for DefinitionSetup<Setup, Mode>
where
    Setup: AktorStart<
            Mode,
            Group = AktorGroup,
            Handles = Actor,
            Startup = AktorThreadFuture<Result<Actor, AktorStartError>>,
        >,
{
    fn begin(&self, group: &mut AktorGroup) -> Result<(), AktorSetupError> {
        self.setup.begin(group)
    }

    fn start(
        self: Box<Self>,
        context: AktorStartContext<AktorGroup>,
    ) -> AktorThreadFuture<Result<Actor, AktorStartError>> {
        self.setup.start_in(context)
    }
}

impl<S: 'static, Start, Kind: AktorMode, Role, End> AktorNew<S, Start, Kind, Role, End> {
    /// Hide the callback types when returning a native actor definition from a helper.
    pub fn definition<Mode, Actor>(self) -> AktorDefinition<Actor>
    where
        Self: AktorStart<
                Mode,
                Group = AktorGroup,
                Handles = Actor,
                Startup = AktorThreadFuture<Result<Actor, AktorStartError>>,
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
impl<Actor> AktorStart for AktorDefinition<Actor> {
    type Group = AktorGroup;
    type Handles = Actor;
    type Startup = AktorThreadFuture<Result<Actor, AktorStartError>>;

    fn begin(&self, group: &mut Self::Group) -> Result<(), AktorSetupError> {
        self.setup.begin(group)
    }

    fn start_in(self, context: AktorStartContext<Self::Group>) -> Self::Startup {
        self.setup.start(context)
    }
}
