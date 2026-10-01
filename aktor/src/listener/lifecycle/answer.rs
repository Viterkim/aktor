use super::*;

pub type ResumeReply<E> = oneshot::Sender<SetupAnswer<Result<(), LifecycleError<E>>, E>>;
pub type ReplaceReply<E, C> = oneshot::Sender<SetupAnswer<Result<(), ReplaceError<E, C>>, E>>;

pub struct SetupAnswer<T, E> {
    result: Option<T>,
    abandoned: Arc<Mutex<Vec<AbandonedSetup<E>>>>,
    retain: fn(T) -> Option<AbandonedSetup<E>>,
}
impl<T, E> SetupAnswer<T, E> {
    pub fn new(
        result: T,
        abandoned: Arc<Mutex<Vec<AbandonedSetup<E>>>>,
        retain: fn(T) -> Option<AbandonedSetup<E>>,
    ) -> Self {
        Self {
            result: Some(result),
            abandoned,
            retain,
        }
    }

    pub fn take(mut self) -> Option<T> {
        self.result.take()
    }
}
impl<T, E> Drop for SetupAnswer<T, E> {
    fn drop(&mut self) {
        // Sending into a oneshot doesn't mean its caller consumed the error.
        if let Some(result) = self.result.take()
            && let Some(error) = (self.retain)(result)
        {
            self.abandoned.lock().push(error);
        }
    }
}
