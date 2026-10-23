use crate::{CallError, Cause, Error};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, watch};

#[derive(Default)]
pub struct Sessions {
    entries: Mutex<HashMap<[u8; 16], Weak<AsyncMutex<Generation>>>>,
}

struct Generation {
    number: u64,
    cancel: watch::Sender<bool>,
    finished: watch::Receiver<()>,
}

pub struct Claim {
    pub session: [u8; 16],
    pub cancel: watch::Receiver<bool>,
    _record: Arc<AsyncMutex<Generation>>,
    _life: watch::Sender<()>,
}

impl Sessions {
    pub async fn claim(
        &self,
        identity: Option<[u8; 16]>,
        number: u64,
    ) -> Result<Option<Claim>, Error> {
        let (identity, record) = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            entries.retain(|_, record| record.strong_count() != 0);

            match identity {
                Some(identity) => {
                    let Some(record) = entries.get(&identity).and_then(Weak::upgrade) else {
                        return Ok(None);
                    };
                    (identity, record)
                }
                None => {
                    let mut identity = [0; 16];
                    getrandom::fill(&mut identity).map_err(|error| {
                        Error::new(CallError::NotAdmitted, Cause::Closed, error.to_string())
                    })?;
                    let record = Arc::new(AsyncMutex::new(Generation {
                        number: 0,
                        cancel: watch::channel(false).0,
                        finished: watch::channel(()).1,
                    }));
                    entries.insert(identity, Arc::downgrade(&record));
                    (identity, record)
                }
            }
        };
        let mut generation = record.lock().await;
        if number <= generation.number {
            return Err(Error::new(
                CallError::NotAdmitted,
                Cause::Protocol,
                "latest connection generation did not advance",
            ));
        }

        generation.cancel.send_replace(true);
        // Acknowledging retirement waits until the old results receiver has been dropped.
        let _ = generation.finished.changed().await;
        let (cancel, cancelled) = watch::channel(false);
        let (life, finished) = watch::channel(());
        *generation = Generation {
            number,
            cancel,
            finished,
        };
        drop(generation);

        Ok(Some(Claim {
            session: identity,
            cancel: cancelled,
            _record: record,
            _life: life,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retired() {
        let sessions = Sessions::default();
        let claim = sessions.claim(None, 1).await.unwrap().unwrap();
        let session = claim.session;
        drop(claim);

        assert!(sessions.claim(Some(session), 2).await.unwrap().is_none());
    }
}
