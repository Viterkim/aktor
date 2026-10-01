use core::panic::Location;

#[derive(Clone, Copy, Debug)]
pub struct Operation {
    pub name: &'static str,
    pub caller: &'static Location<'static>,
}
