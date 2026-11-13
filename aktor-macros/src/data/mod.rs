mod bounds;
mod generate;
mod impls;
mod parse;

pub use generate::expand;
use syn::Type;

#[derive(Clone, Copy)]
pub enum Style {
    Named,
    Tuple,
    Unit,
}

pub struct Member {
    pub name: syn::Member,
    pub ty: Type,
    pub skip: bool,
}

pub struct Shape {
    pub style: Style,
    pub members: Vec<Member>,
}
