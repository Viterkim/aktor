use crate::operation::Operation;
use alloc::{boxed::Box, rc::Rc, vec::Vec};
use core::cell::RefCell;

pub type AktorEach<S> = Box<dyn FnMut(&mut S, Operation)>;

pub type IntervalCallback<S> =
    Rc<RefCell<Option<crate::AktorClosure<dyn crate::setup::AktorIntervalLogic<S>>>>>;

pub struct AktorHooks<S> {
    pub before_each: Option<AktorEach<S>>,
    pub after_each: Option<AktorEach<S>>,
    pub intervals: Vec<IntervalCallback<S>>,
}
impl<S> Default for AktorHooks<S> {
    fn default() -> Self {
        Self {
            before_each: None,
            after_each: None,
            intervals: Vec::new(),
        }
    }
}
impl<S> AktorHooks<S> {
    pub fn before(&mut self, state: &mut S, operation: Operation) {
        if let Some(before) = &mut self.before_each {
            before(state, operation);
        }
    }

    pub fn after(&mut self, state: &mut S, operation: Operation) {
        if let Some(after) = &mut self.after_each {
            after(state, operation);
        }
    }
}
