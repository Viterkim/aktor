use super::*;
use crate::local::hooks::IntervalCallback;
use alloc::rc::{Rc, Weak};
use core::cell::RefCell;

pub struct LocalInterval<S> {
    pub every: Duration,
    pub callback: Weak<RefCell<Option<AktorClosure<dyn AktorIntervalLogic<S>>>>>,
}

pub struct LocalIntervals<S> {
    pub scheduled: Vec<LocalInterval<S>>,
    pub retained: Vec<IntervalCallback<S>>,
}
impl<S: 'static> LocalIntervals<S> {
    pub fn new<Kind: AktorMode<Interval<S> = dyn AktorIntervalLogic<S>>>(
        intervals: Vec<AktorInterval<S, Kind>>,
    ) -> Self {
        let mut retained = Vec::new();
        let scheduled = intervals
            .into_iter()
            .map(|interval| {
                let callback = Rc::new(RefCell::new(Some(interval.run)));

                retained.push(callback.clone());
                LocalInterval {
                    every: interval.every,
                    callback: Rc::downgrade(&callback),
                }
            })
            .collect();

        Self {
            scheduled,
            retained,
        }
    }
}
