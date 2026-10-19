use super::*;
use core::task::{Context, Poll};

impl<Setup: AktorStart<Mode>, Mode> AktorStartup<Setup, Mode> {
    pub fn killswitch(&self) -> <Setup::Group as AktorSetupGroup>::KillSwitch {
        self.killswitch.clone()
    }

    pub fn completion(&self) -> <Setup::Group as AktorSetupGroup>::Completion {
        self.completion.clone()
    }

    fn closing(&mut self, error: AktorStartError) {
        self.failure = Some(Box::new(error));

        if let Some(group) = &self.group {
            Setup::Group::set_starting(&self.killswitch, true);
            Setup::Group::stop(&self.killswitch);
            self.shutdown = Some(Box::pin(group.closing()));
        }
    }
}
impl<Setup: AktorStart<Mode>, Mode> Drop for AktorStartup<Setup, Mode> {
    fn drop(&mut self) {
        if self.group.is_some() {
            Setup::Group::set_starting(&self.killswitch, true);
            Setup::Group::stop(&self.killswitch);
        }
    }
}
impl<Setup: AktorStart<Mode>, Mode> Future for AktorStartup<Setup, Mode> {
    type Output = Result<AktorStarted<Setup::Handles, Setup::Group>, AktorStartupError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(setup) = this.setup.take()
            && let Some(group) = &mut this.group
        {
            let begun = if this.begin {
                setup.begin(group)
            } else {
                group.check_started()
            };

            match begun {
                Ok(_) => {
                    this.starting = Some(Box::pin(setup.start_in(AktorStartContext {
                        group: group.registration(),
                    })));
                }
                Err(error) => this.closing(error.into()),
            }
        }

        if let Some(starting) = &mut this.starting {
            let result = match starting.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };

            this.starting = None;

            match result {
                Ok(handles) => {
                    let initialized = if this.begin {
                        Setup::Group::set_starting(&this.killswitch, false)
                    } else {
                        !Setup::Group::is_stopping(&this.killswitch)
                    };

                    if !initialized {
                        drop(handles);
                        this.closing(
                            AktorSetupError::new("actor group stopped during startup", ()).into(),
                        );
                    } else if let Some(group) = this.group.take() {
                        return Poll::Ready(Ok(AktorStarted { handles, group }));
                    }
                }
                Err(error) => this.closing(error),
            }
        }

        if let Some(shutdown) = &mut this.shutdown {
            let report = match shutdown.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(report) => report,
            };

            this.shutdown = None;

            if let Some(error) = this.failure.take() {
                let diagnostics = Setup::diagnostics(&error);
                let source_is_failure = Setup::source_is_setup(&error)
                    && Setup::Group::source_is_failure(&this.killswitch);

                return Poll::Ready(Err(AktorStartupError::from_failure(
                    *error,
                    diagnostics,
                    Some(Box::new(report)),
                    source_is_failure,
                )));
            }
        }

        Poll::Ready(Err(AktorStartupError::from_failure(
            AktorSetupError::new("actor startup was polled after completion", ()).into(),
            "actor startup was polled after completion".into(),
            None,
            false,
        )))
    }
}
