use super::*;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::io;
use tokio::sync::mpsc;

pub(super) type Pending = Pin<Box<dyn Future<Output = ()> + Send>>;

pub(super) fn drive<F: Future<Output = ShutdownReport>>(
    control: Arc<Control>,
    closing: F,
) -> impl Future<Output = ShutdownReport> {
    let (sender, mut receiver) = mpsc::unbounded_channel();

    *control
        .intervals
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(sender);

    async move {
        let mut intervals = FuturesUnordered::new();

        tokio::pin!(closing);

        loop {
            tokio::select! {
                report = &mut closing => {
                    control.intervals.lock().unwrap_or_else(|error| error.into_inner()).take();
                    return report;
                },
                Some(interval) = receiver.recv() => intervals.push(interval),
                Some(()) = intervals.next(), if !intervals.is_empty() => {},
            }
        }
    }
}

impl AktorGroup {
    pub(crate) fn spawn_interval(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        #[cfg(test)]
        crate::executor::check_spawn("aktor interval")?;

        let sender = self
            .control
            .intervals
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or_else(|| io::Error::other("interval scheduler is not running"))?;

        sender
            .send(Box::pin(future))
            .map_err(|_| io::Error::other("interval scheduler stopped"))
    }
}
