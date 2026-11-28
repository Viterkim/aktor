use crate::{Cause, Rejection};
use er::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, watch};

#[derive(Default)]
pub struct Sessions {
    entries: Mutex<HashMap<[u8; 16], Weak<Session>>>,
}

struct Session {
    generation: AsyncMutex<Generation>,
    _slot: OwnedSemaphorePermit,
}

struct Generation {
    number: u64,
    cancel: watch::Sender<bool>,
    finished: watch::Receiver<()>,
}

pub struct Claim {
    pub session: [u8; 16],
    pub cancel: watch::Receiver<bool>,
    _record: Arc<Session>,
    _life: watch::Sender<()>,
}

#[derive(Er)]
pub enum ClaimSessionErr {
    Capacity,
    Identity,
    Generation { previous: u64, received: u64 },
}
impl ClaimSessionErr {
    pub fn reason(&self) -> Rejection {
        match *self {
            Self::Capacity => Rejection::ConnectionLimit,
            Self::Identity => Rejection::SessionIdentity,
            Self::Generation { previous, received } => {
                Rejection::SessionGeneration { previous, received }
            }
        }
    }

    pub fn cause(&self) -> Cause {
        match self {
            Self::Capacity | Self::Identity => Cause::Closed,
            Self::Generation { .. } => Cause::Protocol,
        }
    }
}
impl Sessions {
    pub async fn claim(
        &self,
        identity: Option<[u8; 16]>,
        number: u64,
        slot: &mut Option<OwnedSemaphorePermit>,
    ) -> ErResult<Option<Claim>, ClaimSessionErr> {
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
                    let Some(slot) = slot.take() else {
                        er_bail!(ClaimSessionErr::Capacity);
                    };
                    let mut identity = [0; 16];

                    getrandom::fill(&mut identity).er(ClaimSessionErr::identity)?;

                    let record = Arc::new(Session {
                        generation: AsyncMutex::new(Generation {
                            number: 0,
                            cancel: watch::channel(false).0,
                            finished: watch::channel(()).1,
                        }),
                        _slot: slot,
                    });
                    entries.insert(identity, Arc::downgrade(&record));
                    (identity, record)
                }
            }
        };
        drop(slot.take());
        let mut generation = record.generation.lock().await;
        if number <= generation.number {
            er_bail!(ClaimSessionErr::Generation {
                previous: generation.number,
                received: number
            });
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
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        let mut slot = Some(capacity.clone().try_acquire_owned().unwrap());
        let claim = sessions
            .claim(None, 1, &mut slot)
            .await
            .er_unwrap()
            .unwrap();
        let session = claim.session;
        drop(claim);

        assert!(
            sessions
                .claim(Some(session), 2, &mut None)
                .await
                .er_unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn replacement_keeps_capacity_reserved() {
        let sessions = Sessions::default();
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        let mut slot = Some(capacity.clone().try_acquire_owned().unwrap());
        let mut first = sessions
            .claim(None, 1, &mut slot)
            .await
            .er_unwrap()
            .unwrap();
        let mut next_slot = None;
        let replacement = sessions.claim(Some(first.session), 2, &mut next_slot);
        tokio::pin!(replacement);
        tokio::select! {
            biased;
            _ = &mut replacement => panic!("replacement took over before retirement"),
            cancelled = first.cancel.changed() => cancelled.unwrap(),
        }

        drop(first);
        assert_eq!(capacity.available_permits(), 0);
        let replacement = replacement.await.er_unwrap().unwrap();
        assert_eq!(capacity.available_permits(), 0);
        drop(replacement);
        assert_eq!(capacity.available_permits(), 1);
    }
}
