use crate::operation::Operation;
use alloc::boxed::Box;

pub type AktorEach<S> = Box<dyn FnMut(&mut S, Operation) + Send>;

pub struct AktorHooks<S> {
    pub before_each: Option<AktorEach<S>>,
    pub after_each: Option<AktorEach<S>>,
}
impl<S> Default for AktorHooks<S> {
    fn default() -> Self {
        Self {
            before_each: None,
            after_each: None,
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
