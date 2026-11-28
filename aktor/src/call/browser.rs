use super::*;
use crate::worker::{WorkerReply, WorkerRequest, WorkerRequestCast, WorkerRequestSend};
use core::time::Duration;

impl<'a, S, O: 'static, Role> SendRequest for WorkerRequest<'a, S, O, Role> {
    type Reply = WorkerReply<O>;
    type Send = WorkerRequestSend<'a, S, O, Role>;

    fn send(self) -> Self::Send {
        self.send()
    }
}
impl<'a, S, O: 'static, Role> TimeoutRequest for WorkerRequest<'a, S, O, Role> {
    type Timed = crate::AktorTimeout<Self>;

    fn timeout(self, duration: Duration) -> Self::Timed {
        self.timeout(duration)
    }
}
impl<'a, S, Role> CastRequest for WorkerRequest<'a, S, (), Role> {
    type Output = Result<(), crate::AktorDeliveryError>;
    type Cast = WorkerRequestCast<'a, S, Role>;

    fn cast(self) -> Self::Cast {
        self.cast()
    }
}
