use super::super::*;
use core::task::ready;

impl<O> Reply<O> {
    pub fn try_take(&mut self) -> Option<Result<O, crate::AktorDeliveryError>> {
        if self.taken {
            return None;
        }

        let output = self.answer.try_take()?;
        self.taken = true;
        Some(match output {
            Ok(value) => Ok(value),
            Err(outcome) => Err(outcome.error(self.receive)),
        })
    }

    pub fn timeout(&mut self, duration: core::time::Duration) -> crate::AktorTimeout<&mut Self> {
        let standard = self.admission.is_standard();

        crate::AktorTimeout::queued(
            self,
            duration,
            |_| crate::timeout::WaitStatus { admitted: true },
            standard,
        )
    }
}
impl<O> Future for Reply<O> {
    type Output = Result<O, crate::AktorDeliveryError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.taken {
            consumed();
        }

        let output = ready!(self.answer.poll(cx));
        self.taken = true;
        Poll::Ready(match output {
            Ok(value) => Ok(value),
            Err(outcome) => Err(outcome.error(self.receive)),
        })
    }
}
impl<O> Drop for Reply<O> {
    fn drop(&mut self) {
        if !self.taken {
            self.answer.abandon();
        }
    }
}
