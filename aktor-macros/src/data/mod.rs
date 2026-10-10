mod bounds;
mod generate;
mod impls;
mod parse;

pub use generate::expand;
use syn::{Fields, Type};

#[derive(Clone)]
pub struct Member {
    pub name: syn::Member,
    pub ty: Type,
    pub skip: bool,
}

#[derive(Clone)]
pub struct Shape {
    pub fields: Fields,
    pub members: Vec<Member>,
}
