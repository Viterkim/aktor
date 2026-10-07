use super::*;
use core::task::{Context, Poll};

impl<Setup: AktorStart> AktorStartup<Setup> {
    pub fn killswitch(&self) -> <Setup::Group as AktorSetupGroup>::KillSwitch {
        self.killswitch.clone()
    }

    pub fn completion(&self) -> <Setup::Group as AktorSetupGroup>::Completion {
        self.completion.clone()
    }

    fn closing(&mut self, error: Setup::Error) {
        self.failure = Some(Box::new(error));

        if let Some(group) = &self.group {
            Setup::Group::set_starting(&self.killswitch, true);
            Setup::Group::stop(&self.killswitch);
            self.shutdown = Some(Box::pin(group.closing()));
        }
    }
}
impl<Setup: AktorStart> Drop for AktorStartup<Setup> {
    fn drop(&mut self) {
        if self.group.is_some() {
            Setup::Group::set_starting(&self.killswitch, true);
            Setup::Group::stop(&self.killswitch);
        }
    }
}
impl<Setup: AktorStart> Future for AktorStartup<Setup> {
    type Output =
        Result<AktorStarted<Setup::Handles, Setup::Group>, AktorStartupError<Setup::Error>>;

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
                            AktorSetupError::new("actor group stopped during startup").into(),
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
                return Poll::Ready(Err(AktorStartupError {
                    source_is_failure: Setup::source_is_setup(&error)
                        && Setup::Group::source_is_failure(&this.killswitch),
                    error: *error,
                    report: Some(Box::new(report)),
                }));
            }
        }

        Poll::Ready(Err(AktorStartupError {
            error: AktorSetupError::new("actor startup was polled after completion").into(),
            report: None,
            source_is_failure: false,
        }))
    }
}
