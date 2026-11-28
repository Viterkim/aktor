mod bounds;
mod generate;
mod impls;
mod parse;

pub use generate::expand;
use syn::{Path, Type, WherePredicate};

/// What the `#[aktor(...)]` on the type asked for.
pub struct Options {
    pub krate: Option<Path>,
    pub bound: Option<Vec<WherePredicate>>,
}

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
    /// `#[aktor(max_len = N)]`, checked before anything gets copied.
    pub max_len: Option<syn::Expr>,
}

pub struct Shape {
    pub style: Style,
    pub members: Vec<Member>,
}
