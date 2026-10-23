use core::hint::spin_loop;
use std::sync::{Mutex as StdMutex, MutexGuard, TryLockError};

pub struct Mutex<T>(StdMutex<T>);
impl<T> Mutex<T> {
    pub const fn new(value: T) -> Self {
        Self(StdMutex::new(value))
    }

    pub fn lock<R>(&self, run: impl FnOnce(&T) -> R) -> R {
        run(&self.guard())
    }

    pub fn guard(&self) -> MutexGuard<'_, T> {
        loop {
            // Browser main cannot wait synchronously on an atomic lock.
            match self.0.try_lock() {
                Ok(guard) => return guard,
                Err(TryLockError::Poisoned(error)) => return error.into_inner(),
                Err(TryLockError::WouldBlock) => spin_loop(),
            }
        }
    }
}
