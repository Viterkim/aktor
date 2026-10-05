use syn::{Block, Ident, Pat, Path, Signature, Type};

pub mod attributes;
pub mod impls;
pub mod parse;
#[cfg(test)]
mod tests;

pub struct Options {
    pub crate_path: Option<Path>,
    pub role: Option<Path>,
}

pub struct Argument {
    pub pattern: Pat,
}

pub struct State {
    pub pattern: Pat,
    pub ty: Type,
    pub mutable: bool,
}

pub struct Function {
    pub attributes: Vec<syn::Attribute>,
    pub visibility: syn::Visibility,
    pub signature: Signature,
    pub body: Box<Block>,
    pub inputs: Vec<Argument>,
}
