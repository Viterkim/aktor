use syn::{Block, Ident, Pat, Path, Signature, Type};

pub mod attributes;
pub mod impls;
pub mod parse;

pub struct Options {
    pub crate_path: Option<Path>,
    pub role: Option<Path>,
    /// The operation's name on the wire and in presets, `module_path!()` when left out.
    pub name: Option<syn::LitStr>,
    pub remote: bool,
    /// `.latest()` on this function. Most functions never use it, so its machinery stays out.
    pub latest: bool,
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
